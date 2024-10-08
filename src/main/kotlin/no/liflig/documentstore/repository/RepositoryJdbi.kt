@file:Suppress("MemberVisibilityCanBePrivate") // This is a library, we want to expose fields

package no.liflig.documentstore.repository

import java.time.Instant
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.getEntityIdType
import no.liflig.documentstore.utils.executeBatchOperation
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
 *   -- Can have type `text` if using `StringEntityId`
 *   id          uuid        NOT NULL PRIMARY KEY,
 *   created_at  timestamptz NOT NULL,
 *   modified_at timestamptz NOT NULL,
 *   version     bigint      NOT NULL,
 *   data        jsonb       NOT NULL
 * );
 * ```
 *
 * Also, the given [Jdbi] instance must have the
 * [DocumentStorePlugin][no.liflig.documentstore.DocumentStorePlugin] installed for the queries in
 * this class to work.
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
      useHandle(jdbi) { handle ->
        val now = Instant.now()
        val version = Version.initial()
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
            .bind("createdAt", now)
            .bind("modifiedAt", now)
            .execute()
        return Versioned(entity, version, createdAt = now, modifiedAt = now)
      }
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
        val modifiedAt = Instant.now()
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

        return Versioned(
            entity,
            nextVersion,
            createdAt = updateResult.createdAt,
            modifiedAt = modifiedAt,
        )
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

  override fun listByIds(ids: List<EntityIdT>): List<Versioned<EntityT>> {
    if (ids.isEmpty()) {
      return emptyList()
    }

    /** See [getEntityIdType]. */
    val elementType = getEntityIdType(ids.first())
    return getByPredicate("id = ANY (:ids)") { bindArray("ids", elementType, ids) }
  }

  override fun listAll(): List<Versioned<EntityT>> {
    return getByPredicate() // Defaults to all
  }

  override fun batchCreate(entities: Iterable<EntityT>) {
    transactional {
      useHandle(jdbi) { handle ->
        val now = Instant.now()
        val version = Version.initial()

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
                  .bind("createdAt", now)
                  .bind("modifiedAt", now)
            },
        )
      }
    }
  }

  override fun batchUpdate(entities: Iterable<Versioned<EntityT>>) {
    transactional {
      useHandle(jdbi) { handle ->
        val now = Instant.now()

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
                  .bind("modifiedAt", now)
                  .bind("id", entity.item.id)
                  .bind("previousVersion", entity.version)
            },
            handleModifiedRowCounts = { counts, batchStartIndex ->
              handleModifiedRowCounts(counts, batchStartIndex, entities, operation = "update")
            },
        )
      }
    }
  }

  override fun batchDelete(entities: Iterable<Versioned<EntityT>>) {
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
   */
  protected open fun getByPredicate(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      forUpdate: Boolean = false,
      bind: Query.() -> Query = { this }
  ): List<Versioned<EntityT>> {
    useHandle(jdbi) { handle ->
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      val limitString = if (limit != null) "LIMIT ${limit}" else ""
      val offsetString = if (offset != null) "OFFSET ${offset}" else ""
      val forUpdateString = if (forUpdate) " FOR UPDATE" else ""

      return handle
          .createQuery(
              """
                SELECT id, data, version, created_at, modified_at
                FROM "${tableName}"
                WHERE (${sqlWhere})
                ORDER BY ${orderByString} ${orderDirection}
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
      bind: Query.() -> Query = { this }
  ): ListWithTotalCount<Versioned<EntityT>> {
    useHandle(jdbi) { handle ->
      val limitString = limit?.let { "LIMIT $it" } ?: ""
      val offsetString = offset?.let { "OFFSET $it" } ?: ""
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

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
                      ORDER BY ${orderByString} ${orderDirection}
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
