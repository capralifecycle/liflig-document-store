@file:Suppress(
    // This is a library, we want to expose fields
    "MemberVisibilityCanBePrivate",
    // We duplicate code some places to make it more visible when jumping to it in the editor
    "DuplicatedCode",
)

package no.liflig.documentstore.repository

import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.getEntityIdType
import no.liflig.documentstore.utils.currentTimeWithMicrosecondPrecision
import no.liflig.documentstore.utils.executeBatchOperation
import no.liflig.documentstore.utils.isEmpty
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query

/**
 * An implementation of [Repository] that uses the [JDBI library](https://jdbi.org/) for database
 * access.
 *
 * Provides default implementations for CRUD (Create, Read, Update, Delete), as well as `listByIds`
 * and `listAll`. To implement filtering on specific fields on your entities, you can extend this
 * and use the [getByPredicate] utility method to provide your own WHERE clause (or
 * [getByPredicateWithTotalCount] for better pagination support).
 *
 * The implementation assumes that the table has the following columns:
 * ```sql
 * CREATE TABLE example
 * (
 *   -- Can have type `text` if using `StringEntityId`, or `bigint` if using `IntegerEntityId`
 *   id          uuid        NOT NULL PRIMARY KEY,
 *   created_at  timestamptz NOT NULL,
 *   modified_at timestamptz NOT NULL,
 *   version     bigint      NOT NULL,
 *   data        jsonb       NOT NULL
 * );
 * ```
 *
 * @param jdbi Must have the [DocumentStorePlugin] installed for the queries in this class to work.
 * @param serializationAdapter See [SerializationAdapter] for an example of how to implement this.
 */
open class RepositoryJdbi<EntityIdT : EntityId, EntityT : Entity<EntityIdT>>(
    protected val jdbi: Jdbi,
    protected val tableName: String,
    protected val serializationAdapter: SerializationAdapter<EntityT>,
) : Repository<EntityIdT, EntityT> {
  protected val rowMapper: RowMapper<Versioned<EntityT>> = EntityRowMapper(serializationAdapter)

  private val rowMapperWithTotalCount: RowMapper<MappedEntityWithTotalCount<EntityT>> =
      EntityRowMapperWithTotalCount(serializationAdapter)

  private val updateResultMapper: RowMapper<UpdateResult> = UpdateResultMapper()

  override fun create(entity: EntityT): Versioned<EntityT> {
    try {
      val createdAt = currentTimeWithMicrosecondPrecision()
      val version = Version.initial()

      useHandle(jdbi) { handle ->
        handle
            .createUpdate(
                """
                  INSERT INTO "${tableName}" (id, data, version, created_at, modified_at)
                  VALUES (:id, :data::jsonb, :version, :createdAt, :modifiedAt)
                """
                    .trimIndent(),
            )
            .bind("id", entity.id)
            .bind("data", serializationAdapter.toJson(entity))
            .bind("version", version)
            .bind("createdAt", createdAt)
            .bind("modifiedAt", createdAt)
            .execute()
      }

      return Versioned(entity, version, createdAt, modifiedAt = createdAt)
    } catch (e: Exception) {
      // Call mapDatabaseException first to handle connection-related exceptions, before calling
      // mapCreateOrUpdateException (which may be overridden by users for custom error handling).
      throw mapCreateOrUpdateException(mapDatabaseException(e), entity)
    }
  }

  override fun get(id: EntityIdT, forUpdate: Boolean): Versioned<EntityT>? {
    useHandle(jdbi) { handle ->
      return handle
          .createQuery(
              """
                SELECT id, data, version, created_at, modified_at
                FROM "${tableName}"
                WHERE id = :id
                ORDER BY created_at
                ${if (forUpdate) " FOR UPDATE" else ""}
              """
                  .trimIndent(),
          )
          .bind("id", id)
          .map(rowMapper)
          .firstOrNull()
    }
  }

  override fun <EntityOrSubClassT : EntityT> update(
      entity: EntityOrSubClassT,
      previousVersion: Version
  ): Versioned<EntityOrSubClassT> {
    try {
      useHandle(jdbi) { handle ->
        val nextVersion = previousVersion.next()
        val modifiedAt = currentTimeWithMicrosecondPrecision()
        val updateResult =
            handle
                .createQuery(
                    /** See [UpdateResult] for why we use RETURNING here. */
                    """
                      UPDATE "${tableName}"
                      SET
                        data = :data::jsonb,
                        version = :nextVersion,
                        modified_at = :modifiedAt
                      WHERE
                        id = :id AND
                        version = :previousVersion
                      RETURNING
                        created_at
                    """
                        .trimIndent(),
                )
                .bind("data", serializationAdapter.toJson(entity))
                .bind("nextVersion", nextVersion)
                .bind("modifiedAt", modifiedAt)
                .bind("id", entity.id)
                .bind("previousVersion", previousVersion)
                .map(updateResultMapper)
                .firstOrNull()

        if (updateResult == null) {
          throw ConflictRepositoryException(
              "Entity was concurrently modified between being retrieved and trying to update it [Entity: ${entity}]",
          )
        }

        return Versioned(entity, nextVersion, updateResult.createdAt, modifiedAt)
      }
    } catch (e: Exception) {
      // Call mapDatabaseException first to handle connection-related exceptions, before calling
      // mapCreateOrUpdateException (which may be overridden by users for custom error handling).
      throw mapCreateOrUpdateException(mapDatabaseException(e), entity)
    }
  }

  override fun delete(id: EntityIdT, previousVersion: Version) {
    useHandle(jdbi) { handle ->
      val deleted =
          handle
              .createUpdate(
                  """
                    DELETE FROM "${tableName}"
                    WHERE
                      id = :id AND
                      version = :previousVersion
                  """
                      .trimIndent(),
              )
              .bind("id", id)
              .bind("previousVersion", previousVersion)
              .execute()

      if (deleted == 0) {
        throw ConflictRepositoryException(
            "Entity was concurrently modified between being retrieved and trying to delete it [Entity ID: ${id}]",
        )
      }
    }
  }

  override fun listByIds(ids: List<EntityIdT>, forUpdate: Boolean): List<Versioned<EntityT>> {
    if (ids.isEmpty()) {
      return emptyList()
    }

    return getByPredicate(
        "id = ANY (:ids)",
        forUpdate = forUpdate,
    ) {
      /** See [getEntityIdType]. */
      bindArray("ids", getEntityIdType(ids.first()), ids)
    }
  }

  override fun listAll(): List<Versioned<EntityT>> {
    return getByPredicate() // Defaults to all
  }

  override fun batchCreate(entities: Iterable<EntityT>): List<Versioned<EntityT>> {
    if (entities.isEmpty()) {
      return emptyList()
    }

    val createdAt = currentTimeWithMicrosecondPrecision()
    val version = Version.initial()

    transactional {
      useHandle(jdbi) { handle ->
        executeBatchOperation(
            handle,
            entities,
            statement =
                """
                  INSERT INTO "${tableName}" (id, data, version, created_at, modified_at)
                  VALUES (:id, :data::jsonb, :version, :createdAt, :modifiedAt)
                """
                    .trimIndent(),
            bindParameters = { batch, entity ->
              batch
                  .bind("id", entity.id)
                  .bind("data", serializationAdapter.toJson(entity))
                  .bind("version", version)
                  .bind("createdAt", createdAt)
                  .bind("modifiedAt", createdAt)
            },
        )
      }
    }

    // We wait until here to create the result list, which may be large, to avoid allocating it
    // before calling the database. That would keep the list in memory while we are waiting for the
    // database, needlessly reducing throughput.
    return entities.map { entity ->
      Versioned(entity, version, createdAt = createdAt, modifiedAt = createdAt)
    }
  }

  override fun batchUpdate(entities: Iterable<Versioned<EntityT>>): List<Versioned<EntityT>> {
    if (entities.isEmpty()) {
      return emptyList()
    }

    val modifiedAt = currentTimeWithMicrosecondPrecision()

    transactional {
      useHandle(jdbi) { handle ->
        executeBatchOperation(
            handle,
            entities,
            statement =
                """
                  UPDATE "${tableName}"
                  SET
                    data = :data::jsonb,
                    version = :nextVersion,
                    modified_at = :modifiedAt
                  WHERE
                    id = :id AND
                    version = :previousVersion
                """
                    .trimIndent(),
            bindParameters = { batch, entity ->
              val nextVersion = entity.version.next()

              batch
                  .bind("data", serializationAdapter.toJson(entity.item))
                  .bind("nextVersion", nextVersion)
                  .bind("modifiedAt", modifiedAt)
                  .bind("id", entity.item.id)
                  .bind("previousVersion", entity.version)
            },
            handleModifiedRowCounts = { counts, batchStartIndex ->
              handleModifiedRowCounts(counts, batchStartIndex, entities, operation = "update")
            },
        )
      }
    }

    // We wait until here to create the result list, which may be large, to avoid allocating it
    // before calling the database. That would keep the list in memory while we are waiting for the
    // database, needlessly reducing throughput.
    return entities.map { entity ->
      entity.copy(modifiedAt = modifiedAt, version = entity.version.next())
    }
  }

  override fun batchDelete(entities: Iterable<Versioned<EntityT>>) {
    if (entities.isEmpty()) {
      return
    }

    transactional {
      useHandle(jdbi) { handle ->
        executeBatchOperation(
            handle,
            entities,
            statement =
                """
                  DELETE FROM "${tableName}"
                  WHERE
                    id = :id AND
                    version = :previousVersion
                """
                    .trimIndent(),
            bindParameters = { batch, entity ->
              batch.bind("id", entity.item.id).bind("previousVersion", entity.version)
            },
            handleModifiedRowCounts = { counts, batchStartIndex ->
              handleModifiedRowCounts(counts, batchStartIndex, entities, operation = "delete")
            },
        )
      }
    }
  }

  /**
   * Runs a SELECT query using the given WHERE clause, limit, offset etc.
   *
   * When using parameters in [sqlWhere], you must remember to bind them through the [bind] function
   * argument (do not concatenate user input directly in [sqlWhere], as that exposes you to SQL
   * injections). When using a list parameter, use the [Query.bindArray] method and pass the class
   * of the list's elements as the second argument (required for JDBI's reflection to work - see
   * example below).
   *
   * [sqlWhere] will typically use Postgres JSON operators to filter on entity fields, since the
   * document store uses `jsonb` to store entities (see
   * [Postgres docs](https://www.postgresql.org/docs/16/functions-json.html)).
   *
   * Example implementing a query where we want to look up users from a list of names:
   * ```
   * fun getByNames(names: List<String>): List<Versioned<User>> {
   *   return getByPredicate("data->>'name' = ANY(:names)") {
   *     bindArray("names", String::class.java, names)
   *   }
   * }
   * ```
   *
   * @param nullsFirst Controls whether to use
   *   [`NULLS FIRST` or `NULLS LAST`](https://www.postgresql.org/docs/17/queries-order.html) in the
   *   `ORDER BY` clause. The default behavior in Postgres is `NULLS FIRST` when the order direction
   *   is `DESC`, and `NULLS LAST` otherwise, so we keep that behavior here.
   *
   *   If you use this together with a nullable JSONB field in `orderBy`, you may also want to use
   *   [handleJsonNullsInOrderBy].
   *
   * @param handleJsonNullsInOrderBy One quirk with JSONB in Postgres is that a `null` JSON value is
   *   not the same as a `NULL` in SQL. This matters when sorting on a JSON field in [orderBy],
   *   since that sorts SQL `NULL`s first/last (depending on [nullsFirst]), but not JSON `null`s.
   *   This parameter makes `ORDER BY` treat JSON `null`s as SQL `NULL`s, so it works as expected
   *   with e.g. [nullsFirst]. It is ignored if [orderBy] is `null`.
   *
   *   If you use this, you should use `->` rather than `->>` as the JSON field selector in
   *   [orderBy]. This is because `->>` converts the JSON field into a string, which means that the
   *   string `"null"` will be treated as `null`.
   *
   * @param forUpdate Set this to true to lock the rows of the returned entities in the database
   *   until a subsequent call to [update]/[delete], preventing concurrent modification. This only
   *   works when done inside a transaction (see [transactional]).
   */
  protected open fun getByPredicate(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      nullsFirst: Boolean = orderDesc,
      handleJsonNullsInOrderBy: Boolean = false,
      forUpdate: Boolean = false,
      bind: Query.() -> Query = { this }
  ): List<Versioned<EntityT>> {
    useHandle(jdbi) { handle ->
      val orderByString: String =
          when {
            orderBy == null -> Columns.CREATED_AT
            handleJsonNullsInOrderBy -> "NULLIF(${orderBy}, 'null')"
            else -> orderBy
          }
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderNulls = if (nullsFirst) "NULLS FIRST" else "NULLS LAST"

      val limitString = if (limit != null) "LIMIT ${limit}" else ""
      val offsetString = if (offset != null) "OFFSET ${offset}" else ""
      val forUpdateString = if (forUpdate) " FOR UPDATE" else ""

      return handle
          .createQuery(
              """
                SELECT id, data, version, created_at, modified_at
                FROM "${tableName}"
                WHERE (${sqlWhere})
                ORDER BY ${orderByString} ${orderDirection} ${orderNulls}
                ${limitString}
                ${offsetString}
                ${forUpdateString}
              """
                  .trimIndent(),
          )
          .bind()
          .map(rowMapper)
          .list()
    }
  }

  /**
   * Gets database objects matching the given parameters, and the total count of objects matching
   * the WHERE clause without `limit`.
   *
   * This can be used for pagination: for example, if passing e.g. `limit = 10` to display 10 items
   * in a page at a time, the total count can be used to display the number of pages.
   *
   * See [getByPredicate] for further documentation.
   */
  protected open fun getByPredicateWithTotalCount(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      nullsFirst: Boolean = orderDesc,
      handleJsonNullsInOrderBy: Boolean = false,
      bind: Query.() -> Query = { this }
  ): ListWithTotalCount<Versioned<EntityT>> {
    useHandle(jdbi) { handle ->
      val orderByString: String =
          when {
            orderBy == null -> Columns.CREATED_AT
            handleJsonNullsInOrderBy -> "NULLIF(${orderBy}, 'null')"
            else -> orderBy
          }
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderNulls = if (nullsFirst) "NULLS FIRST" else "NULLS LAST"

      val limitString = limit?.let { "LIMIT $it" } ?: ""
      val offsetString = offset?.let { "OFFSET $it" } ?: ""

      val rows =
          handle
              .createQuery(
                  // SQL query based on https://stackoverflow.com/a/28888696
                  // Uses a RIGHT JOIN with the count in order to still get the count when no rows
                  // are returned.
                  """
                    WITH base_query AS (
                      SELECT id, data, version, created_at, modified_at
                      FROM "${tableName}"
                      WHERE (${sqlWhere})
                    )
                    SELECT id, data, version, created_at, modified_at, count
                    FROM (
                      TABLE base_query
                      ORDER BY ${orderByString} ${orderDirection} ${orderNulls}
                      ${limitString}
                      ${offsetString}
                    ) sub_query
                    RIGHT JOIN (
                      SELECT count(*) FROM base_query
                    ) c(count) ON true
                  """
                      .trimIndent(),
              )
              .bind()
              .map(rowMapperWithTotalCount)
              .list()

      val entities = rows.mapNotNull { row -> row.entity }
      val totalCount =
          rows.firstOrNull()?.totalCount
          /**
           * Should never happen: the query should always return 1 row with the count, even if the
           * results are empty (see [MappedEntityWithTotalCount]).
           */
          ?: throw IllegalStateException("Failed to get total count of objects in search query")

      return ListWithTotalCount(entities, totalCount)
    }
  }

  override fun <ReturnT> transactional(block: () -> ReturnT): ReturnT {
    return transactional(jdbi, block)
  }

  /**
   * Method that you can override to map exceptions thrown in [create] or [update] to your own
   * exception type. This is useful to handle e.g. unique constraint violations: instead of letting
   * the database throw an opaque `PSQLException` that may be difficult to handle in layers above,
   * you can instead check if the given exception is a unique index violation and map it to a more
   * useful exception type here.
   *
   * If your implementation receives an exception here that it does not want to map, it should just
   * return it as-is.
   *
   * The entity that was attempted to be created or updated is also provided here, so you can add
   * extra context to the mapped exception.
   *
   * Example:
   * ```
   * override fun mapCreateOrUpdateException(e: Exception, entity: ExampleEntity): Exception {
   *   val message = e.message
   *   if (message != null && message.contains("example_unique_field_index")) {
   *     return UniqueFieldAlreadyExists(entity, cause = e)
   *   }
   *
   *   return e
   * }
   * ```
   */
  protected open fun mapCreateOrUpdateException(e: Exception, entity: EntityT): Exception {
    return e
  }

  /**
   * [batchUpdate] and [batchDelete] use optimistic locking: we only update/delete an entity if it
   * matches an expected [Version] - if it does not, then it likely has been concurrently modified
   * in the meantime, in which case we roll back the batch operation and throw a
   * [ConflictRepositoryException]. We check this by going through the modified row counts retrieved
   * in [executeBatchOperation]. This method handles this logic for both of those methods.
   */
  private fun handleModifiedRowCounts(
      modifiedRowCounts: IntArray,
      batchStartIndex: Int,
      entities: Iterable<Versioned<EntityT>>,
      operation: String,
  ) {
    for (count in modifiedRowCounts.withIndex()) {
      if (count.value == 0) {
        var exceptionMessage =
            "Entity was concurrently modified between being retrieved and trying to ${operation} it in batch ${operation} (rolling back batch ${operation})"
        // We want to add the entity to the exception message for context, but we can only do this
        // if the Iterable is indexable
        if (entities is List) {
          val entity = entities.getOrNull(batchStartIndex + count.index)
          if (entity != null) {
            exceptionMessage += " [Entity: ${entity}]"
          }
        }

        throw ConflictRepositoryException(exceptionMessage)
      }
    }
  }
}
