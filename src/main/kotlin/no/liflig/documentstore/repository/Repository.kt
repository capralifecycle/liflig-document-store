@file:Suppress("MemberVisibilityCanBePrivate", "unused") // This is a library

package no.liflig.documentstore.repository

import java.time.Instant
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityList
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import no.liflig.documentstore.entity.getEntityIdType
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Query

/** Interface for interacting with entities in a database table. */
interface Repository<EntityIdT : EntityId, EntityT : Entity<EntityIdT>> {
  fun create(entity: EntityT): VersionedEntity<EntityT>

  fun get(id: EntityIdT, forUpdate: Boolean = false): VersionedEntity<EntityT>?

  // Uses a generic argument, so that a sub-type can be passed in and be returned as its proper
  // type.
  fun <EntityOrSubClassT : EntityT> update(
    entity: EntityOrSubClassT,
    previousVersion: Version,
  ): VersionedEntity<EntityOrSubClassT>

  fun delete(id: EntityIdT, previousVersion: Version)

  fun listByIds(ids: List<EntityIdT>): EntityList<EntityT>

  fun listAll(): EntityList<EntityT>
}

/**
 * An implementation of [Repository] that uses the [JDBI library](https://jdbi.org/) for database
 * access.
 *
 * Provides default implementations for CRUD (Create, Read, Update, Delete), as well as `listByIds`
 * and `listAll`. To implement filtering on specific fields on your entities, you can extend this
 * and use the [getByPredicate] utility method to provide your own WHERE clause (or
 * [getByPredicateWithTotalCount] for better pagination support).
 */
open class RepositoryJdbi<EntityIdT : EntityId, EntityT : Entity<EntityIdT>>(
  protected val jdbi: Jdbi,
  protected val tableName: String,
  protected val serializationAdapter: SerializationAdapter<EntityT>,
) : Repository<EntityIdT, EntityT> {
  private val rowMapper = createRowMapper(serializationAdapter::fromJson)

  private val rowMapperWithTotalCount =
      createRowMapperWithTotalCount(serializationAdapter::fromJson)

  override fun get(id: EntityIdT, forUpdate: Boolean): VersionedEntity<EntityT>? {
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

  override fun create(entity: EntityT): VersionedEntity<EntityT> {
    try {
      useHandle(jdbi) { handle ->
        val now = Instant.now()
        val version = Version.initial()
        handle
            .createUpdate(
                """
                  INSERT INTO "${tableName}" (id, version, data, modified_at, created_at)
                  VALUES (:id, :version, :data::jsonb, :modifiedAt, :createdAt)
                """
                    .trimIndent(),
            )
            .bind("id", entity.id)
            .bind("version", version)
            .bind("data", serializationAdapter.toJson(entity))
            .bind("modifiedAt", now)
            .bind("createdAt", now)
            .execute()
        return VersionedEntity(entity, version)
      }
    } catch (e: Exception) {
      // Call mapDatabaseException first to handle connection-related exceptions, before calling
      // mapCreateOrUpdateException (which may be overridden by users for custom error handling).
      throw mapCreateOrUpdateException(mapDatabaseException(e), entity)
    }
  }

  override fun <EntityOrSubClassT : EntityT> update(
    entity: EntityOrSubClassT,
    previousVersion: Version
  ): VersionedEntity<EntityOrSubClassT> {
    try {
      useHandle(jdbi) { handle ->
        val nextVersion = previousVersion.next()
        val updated =
            handle
                .createUpdate(
                    """
                      UPDATE "${tableName}"
                      SET
                        version = :nextVersion,
                        data = :data::jsonb,
                        modified_at = :modifiedAt
                      WHERE
                        id = :id AND
                        version = :previousVersion
                    """
                        .trimIndent(),
                )
                .bind("nextVersion", nextVersion)
                .bind("data", serializationAdapter.toJson(entity))
                .bind("id", entity.id)
                .bind("modifiedAt", Instant.now())
                .bind("previousVersion", previousVersion)
                .execute()

        if (updated == 0) {
          throw ConflictRepositoryException(
              "Entity with ID '${entity.id}' was concurrently modified between being retrieved and trying to update it here",
          )
        }

        return VersionedEntity(entity, nextVersion)
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
                    WHERE id = :id AND version = :previousVersion
                  """
                      .trimIndent(),
              )
              .bind("id", id)
              .bind("previousVersion", previousVersion)
              .execute()

      if (deleted == 0) {
        throw ConflictRepositoryException(
            "Entity with ID '${id}' was concurrently modified between being retrieved and trying to delete it here",
        )
      }
    }
  }

  override fun listByIds(ids: List<EntityIdT>): EntityList<EntityT> {
    if (ids.isEmpty()) {
      return emptyList()
    }

    /** See [getEntityIdType]. */
    val elementType = getEntityIdType(ids.first())
    return getByPredicate("id = ANY (:ids)") { bindArray("ids", elementType, ids) }
  }

  override fun listAll(): EntityList<EntityT> {
    return getByPredicate() // Defaults to all
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
   * fun getByNames(names: List<String>): EntityList<User> {
   *     return getByPredicate("data->>'name' = ANY(:names)") {
   *         bindArray("names", String::class.java, names)
   *     }
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
  ): EntityList<EntityT> {
    useHandle(jdbi) { handle ->
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      val limitString = if (limit != null) "LIMIT ${limit}" else ""
      val offsetString = if (offset != null) "OFFSET ${offset}" else ""
      val forUpdateString = if (forUpdate) " FOR UPDATE" else ""

      return handle
          .select(
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
  ): ListWithTotalCount<VersionedEntity<EntityT>> {
    useHandle(jdbi) { handle ->
      val limitString = limit?.let { "LIMIT $it" } ?: ""
      val offsetString = offset?.let { "OFFSET $it" } ?: ""
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      val rows =
          handle
              .select(
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
      val count =
          rows.firstOrNull()?.count
          // Should never happen: the query should always return 1 row with the count, even if the
          // results are empty (see [EntityRowWithCount])
            ?: throw IllegalStateException("Failed to get total count of objects in search query")

      return ListWithTotalCount(entities, count)
    }
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
}
