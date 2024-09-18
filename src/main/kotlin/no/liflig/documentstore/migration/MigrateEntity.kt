package no.liflig.documentstore.migration

import java.sql.Connection
import java.time.Instant
import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.EntityRowMapper
import no.liflig.documentstore.repository.SerializationAdapter
import no.liflig.documentstore.utils.executeBatchOperation
import org.jdbi.v3.core.Jdbi

/**
 * When using a document store, the application must take care to stay backwards-compatible with
 * entities that are stored in the database. Thus, new fields added to entitites must always have a
 * default value, so that entities stored before the field was added can still be deserialized.
 *
 * Sometimes we may add a field with a default value, but also want to add the field to the actual
 * stored entities. For example, we may want to query on the field with
 * [getByPredicate][no.liflig.documentstore.repository.RepositoryJdbi.getByPredicate] without having
 * to handle the field not being present. This function exists for those cases, when you want to
 * migrate the stored entities of a table. It works by reading out all entities from the table, and
 * then writing them back. In the process of this deserialization and re-serialization, default
 * values will be populated. You can perform further transformations with the [transformEntity]
 * parameter.
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
 * File named `V001_1__Test_migration.kt`, under `src/main/kotlin/migrations`. If you've configured
 * "migrations" as a Flyway location, it will automatically find this and run it when you call
 * `Flyway.migrate()`.
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
 * class V001_1__Test_migration : BaseJavaMigration() {
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
 * ## Batch size
 *
 * The migration of the table is done in a streaming fashion, to avoid reading the whole table into
 * memory. It operates on [MIGRATION_BATCH_SIZE] number of entities at a time. According to Oracle,
 * [the optimal size for batch operations in JDBC is 50-100](https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754).
 * We default to the higher end of the recommended batch size for migrations, since migrations are
 * called on startup when there is presumably less memory contention.
 */
fun <EntityT : Entity<*>> migrateEntity(
    dbConnection: Connection,
    tableName: String,
    serializationAdapter: SerializationAdapter<EntityT>,
    transformEntity: ((Versioned<EntityT>) -> EntityT)? = null,
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
                    FROM "${tableName}"
                    FOR UPDATE
                  """
                    .trimIndent(),
            )
            // If we don't specify fetch size, the JDBC driver for Postgres fetches all results
            // by default:
            // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
            // This can lead to out-of-memory errors here, since we fetch the entire table.
            // We really only want to fetch out a single batch at a time, since we process
            // entities in batches. To do that, we set the fetch size, which is the number of
            // rows to fetch at a time. Solution found here:
            // https://www.postgresql.org/message-id/BANLkTi=Df1CR72Bx0L8CBZWBcSfwpmnc-g@mail.gmail.com
            .setFetchSize(MIGRATION_BATCH_SIZE)
            .map(EntityRowMapper(serializationAdapter))

    val modifiedAt = Instant.now()

    executeBatchOperation(
        handle,
        entities,
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
          val updatedEntity = if (transformEntity == null) entity.item else transformEntity(entity)

          batch
              .bind("data", serializationAdapter.toJson(updatedEntity))
              .bind("nextVersion", nextVersion)
              .bind("modifiedAt", modifiedAt)
              .bind("id", entity.item.id)
        },
        batchSize = MIGRATION_BATCH_SIZE,
    )
  }
}

private const val MIGRATION_BATCH_SIZE = 100
