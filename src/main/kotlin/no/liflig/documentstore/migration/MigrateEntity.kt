package no.liflig.documentstore.migration

import java.sql.Connection
import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.EntityRowMapper
import no.liflig.documentstore.repository.RepositoryJdbi
import no.liflig.documentstore.repository.SerializationAdapter
import no.liflig.documentstore.utils.BatchProvider
import no.liflig.documentstore.utils.currentTimeWithMicrosecondPrecision
import no.liflig.documentstore.utils.executeBatchOperation
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Query

/**
 * When using a document store, the application must take care to stay backwards-compatible with
 * entities that are stored in the database. Thus, new fields added to entitites must always have a
 * default value, so that entities stored before the field was added can still be deserialized.
 *
 * Sometimes we may add a field with a default value, but also want to add the field to the actual
 * stored entities. For example, we may want to query on the field with
 * [RepositoryJdbi.getByPredicate] without having to handle the field not being present. This
 * function exists for those cases, when you want to migrate the stored entities of a table. It
 * works by reading out all entities from the table, and then writing them back. In the process of
 * this deserialization and re-serialization, default values will be populated. You can perform
 * further transformations with the [transform] parameter.
 *
 * If you want to migrate only some entities in the table, pass a WHERE clause in the optional
 * [where] parameter, either with static parameters in the clause or with bound parameters in
 * [bindParameters]. This works like [RepositoryJdbi.getByPredicate].
 *
 * ## Usage with Flyway
 *
 * Calling this function on application startup may be problematic, since any additional server
 * instances will call the migration one time each. If you use Flyway for migrations, you should
 * instead call this from a
 * ["Java-based migration"](https://documentation.red-gate.com/fd/tutorial-java-based-migrations-184127624.html).
 * This way, Flyway ensures that the migration is done once, and only once, for the database table.
 *
 * Flyway also automatically runs the migration class in a transaction, so we don't start a new
 * transaction inside here. If you call this without Flyway, you must make sure to call this from
 * within a transaction.
 *
 * ### Example
 *
 * File named `V001_1__Test_migration.kt` (will get version number 1.1), under
 * `src/main/kotlin/migrations`. If you've configured "migrations" as a Flyway location, it will
 * automatically find this and run it when you call `Flyway.migrate()`.
 *
 * For an implementation of `KotlinSerialization`, see [SerializationAdapter].
 *
 * ```
 * package migrations
 *
 * import no.liflig.documentstore.migration.migrateEntity
 * import org.flywaydb.core.api.migration.BaseJavaMigration
 * import org.flywaydb.core.api.migration.Context
 *
 * @Suppress("unused", "ClassName") // Flyway uses this, and expects this naming convention
 * class V001_1__Example_migration : BaseJavaMigration() {
 *   override fun migrate(context: Context) {
 *     migrateEntity(
 *         context.connection,
 *         tableName = "example",
 *         serializationAdapter = KotlinSerialization(ExampleEntity.serializer()),
 *     )
 *   }
 * }
 * ```
 *
 * ## Batching
 *
 * The migration of the table is done in a streaming fashion, to avoid reading the whole table into
 * memory. It operates on [MIGRATION_BATCH_SIZE] number of entities at a time. According to Oracle,
 * [the optimal size for batch operations in JDBC is 50-100](https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754).
 * We use the higher end of the recommended batch size for migrations, since migrations are called
 * on startup when there is presumably less memory contention.
 */
fun <EntityT : Entity<*>> migrateEntity(
    dbConnection: Connection,
    tableName: String,
    serializationAdapter: SerializationAdapter<EntityT>,
    transform: ((Versioned<EntityT>) -> EntityT)? = null,
    where: String? = null,
    bindParameters: Query.() -> Query = { this },
) {
  // We can use migrateEntitySerialization's implementation here, but with
  // CurrentEntityT == TargetEntityT
  migrateEntitySerialization<EntityT, EntityT>(
      dbConnection,
      tableName = tableName,
      currentSerializationAdapter = serializationAdapter,
      targetSerializationAdapter = serializationAdapter,
      transform = transform ?: { entity -> entity.data }, // Default no-op
      where = where,
      bindParameters = bindParameters,
  )
}

/**
 * Variant of [migrateEntity] for when you want to change the serialized form of entities in a
 * database table. It reads out all entities from the table as [CurrentEntityT] (using
 * [currentSerializationAdapter]), applies the given [transform] to turn them into [TargetEntityT],
 * and writes them back with [targetSerializationAdapter].
 *
 * Note that if there is an older server instance still running after this migration has completed,
 * and it reads entities as [CurrentEntityT], it may fail if that type is not compatible with
 * [TargetEntityT]. For these cases, you may instead want to go for an "expand-contract" strategy:
 * 1. Add new fields to the entity, with default values (e.g. defaulting to values of old fields
 *    that you want to replace)
 * 2. Run a [migrateEntity] migration to serialize those new fields on entities in the database
 * 3. Remove old fields, if they've been replaced by the new fields
 * 4. Run another [migrateEntity] migration, to remove the old fields from the serialized entities
 *    in the database
 *
 * [migrateEntitySerialization] is more appropriate for the cases where you don't need to be
 * compatible with parallel server instances, and you don't want to bother with running 2
 * migrations.
 *
 * See [migrateEntity] for more on how to use the other arguments to this function.
 */
fun <CurrentEntityT : Entity<*>, TargetEntityT : Entity<*>> migrateEntitySerialization(
    dbConnection: Connection,
    tableName: String,
    currentSerializationAdapter: SerializationAdapter<CurrentEntityT>,
    targetSerializationAdapter: SerializationAdapter<TargetEntityT>,
    transform: (Versioned<CurrentEntityT>) -> TargetEntityT,
    where: String? = null,
    bindParameters: Query.() -> Query = { this },
) {
  /**
   * When using Java-based migrations with Flyway, we receive a Context with a plain
   * [java.sql.Connection]. However, we use the JDBI library for database access in Liflig Document
   * Store, and want to use that here as well, so we don't have to drop down to the lower-level JDBC
   * APIs just for migrations. Luckily, [Jdbi.create] has an overload that takes a
   * [java.sql.Connection]. When calling [Jdbi.open] below, this single connection will be used for
   * the JDBI handle.
   */
  val jdbi = Jdbi.create(dbConnection).installPlugin(DocumentStorePlugin())

  jdbi.open().use { handle ->
    /**
     * The [org.jdbi.v3.core.result.ResultIterable] must be closed after iterating - but that is
     * automatically done after iterating through all results, which we do in
     * [executeBatchOperation] below.
     */
    val entities =
        handle
            .createQuery(
                """
                  SELECT id, data, version, created_at, modified_at
                  FROM "${tableName}"${if (where != null) " WHERE (${where})" else ""}
                  FOR UPDATE
                """
                    .trimIndent(),
            )
            .bindParameters()
            // If we don't specify fetch size, the JDBC driver for Postgres fetches all results
            // by default:
            // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
            // This can lead to out-of-memory errors here, since we fetch the entire table.
            // We really only want to fetch out a single batch at a time, since we process
            // entities in batches. To do that, we set the fetch size, which is the number of
            // rows to fetch at a time. Solution found here:
            // https://www.postgresql.org/message-id/BANLkTi=Df1CR72Bx0L8CBZWBcSfwpmnc-g@mail.gmail.com
            .setFetchSize(MIGRATION_BATCH_SIZE)
            .map(EntityRowMapper(currentSerializationAdapter))

    val modifiedAt = currentTimeWithMicrosecondPrecision()

    executeBatchOperation(
        handle,
        BatchProvider.fromIterable(entities),
        // We don't have to check version here, since we use FOR UPDATE above, so we know we
        // have the latest version
        statement =
            """
              UPDATE "${tableName}"
              SET
                data = :data::jsonb,
                version = :nextVersion,
                modified_at = :modifiedAt
              WHERE
                id = :id
            """
                .trimIndent(),
        bindParameters = { batch, entity ->
          val nextVersion = entity.version.next()
          val updatedEntity = transform(entity)

          batch
              .bind("data", targetSerializationAdapter.toJson(updatedEntity))
              .bind("nextVersion", nextVersion)
              .bind("modifiedAt", modifiedAt)
              .bind("id", entity.data.id)
        },
        batchSize = MIGRATION_BATCH_SIZE,
    )
  }
}

private const val MIGRATION_BATCH_SIZE = 100
