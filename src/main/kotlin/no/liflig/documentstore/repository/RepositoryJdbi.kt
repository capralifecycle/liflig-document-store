// We use Kotlin Contracts in `transactional`, for ergonomic use with lambdas. Contracts are an
// experimental feature, but they guarantee binary compatibility, so we can safely use them here
@file:OptIn(ExperimentalContracts::class)
@file:Suppress(
    // This is a library, we want to expose fields
    "MemberVisibilityCanBePrivate",
    // We duplicate code some places to make it more visible when jumping to it in the editor
    "DuplicatedCode",
)

package no.liflig.documentstore.repository

import java.util.stream.Stream
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.getEntityIdType
import no.liflig.documentstore.utils.BatchOperationException
import no.liflig.documentstore.utils.BatchProvider
import no.liflig.documentstore.utils.arrayListWithCapacity
import no.liflig.documentstore.utils.currentTimeWithMicrosecondPrecision
import no.liflig.documentstore.utils.executeBatchOperation
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.result.ResultIterable
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

  private val countMapper: RowMapper<Long> = CountMapper()

  override fun create(entity: EntityT): Versioned<EntityT> {
    try {
      val createdAt = currentTimeWithMicrosecondPrecision()
      val version = Version.initial()

      useHandle { handle ->
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
    useHandle { handle ->
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
      previousVersion: Version,
  ): Versioned<EntityOrSubClassT> {
    try {
      useHandle { handle ->
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
    useHandle { handle ->
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

  /**
   * Gives you a stream of all entities in the database table, through the given [useStream]
   * argument. The stream must only be used inside the scope of [useStream] - after it returns, the
   * result stream is closed and associated database resources are released (this is done in an
   * exception-safe way, so database resources are never wasted).
   *
   * @return The same as [useStream] (a generic return type based on the passed lambda).
   */
  fun <ReturnT> streamAll(useStream: (Stream<Versioned<EntityT>>) -> ReturnT): ReturnT {
    return streamByPredicate(useStream) // Defaults to all
  }

  override fun batchCreate(entities: Iterable<EntityT>): List<Versioned<EntityT>> {
    val batchProvider = BatchProvider.fromIterable(entities)
    val results = arrayListWithCapacity<Versioned<EntityT>>(batchProvider.totalSize())

    batchCreateInternal(batchProvider, results)

    return results
  }

  override fun batchCreate(entities: Iterator<EntityT>) {
    batchCreateInternal(BatchProvider.fromIterator(entities))
  }

  private fun batchCreateInternal(
      batchProvider: BatchProvider<EntityT>,
      results: MutableCollection<Versioned<EntityT>>? = null,
  ) {
    if (batchProvider.isEmpty()) {
      return
    }

    val version = Version.initial()
    val createdAt = currentTimeWithMicrosecondPrecision()

    try {
      transactional {
        useHandle { handle ->
          executeBatchOperation(
              handle,
              batchProvider,
              statement =
                  """
                    INSERT INTO "${tableName}" (id, data, version, created_at, modified_at)
                    VALUES (:id, :data::jsonb, :version, :createdAt, :modifiedAt)
                  """
                      .trimIndent(),
              bindParameters = { batch, entity ->
                if (results != null) {
                  results.add(
                      Versioned(
                          entity,
                          version,
                          createdAt = createdAt,
                          modifiedAt = createdAt,
                      ),
                  )
                }

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
    } catch (e: BatchOperationException) {
      @Suppress("UNCHECKED_CAST") // We know the entity is EntityT on this repository
      throw mapCreateOrUpdateException(e.cause, e.entity as EntityT)
    }
  }

  override fun batchUpdate(entities: Iterable<Versioned<EntityT>>): List<Versioned<EntityT>> {
    val batchProvider = BatchProvider.fromIterable(entities)
    val results = arrayListWithCapacity<Versioned<EntityT>>(batchProvider.totalSize())

    batchUpdateInternal(batchProvider, results)

    return results
  }

  override fun batchUpdate(entities: Iterator<Versioned<EntityT>>) {
    batchUpdateInternal(BatchProvider.fromIterator(entities))
  }

  private fun batchUpdateInternal(
      batchProvider: BatchProvider<Versioned<EntityT>>,
      results: MutableCollection<Versioned<EntityT>>? = null,
  ) {
    if (batchProvider.isEmpty()) {
      return
    }

    val modifiedAt = currentTimeWithMicrosecondPrecision()

    try {
      transactional {
        useHandle { handle ->
          executeBatchOperation(
              handle,
              batchProvider,
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
                val newVersion = entity.version.next()

                if (results != null) {
                  results.add(
                      Versioned(
                          entity.data,
                          version = newVersion,
                          createdAt = entity.createdAt,
                          modifiedAt = modifiedAt,
                      ),
                  )
                }

                batch
                    .bind("data", serializationAdapter.toJson(entity.data))
                    .bind("nextVersion", newVersion)
                    .bind("modifiedAt", modifiedAt)
                    .bind("id", entity.data.id)
                    .bind("previousVersion", entity.version)
              },
              handleModifiedRowCounts = { counts, batch ->
                handleModifiedRowCounts(counts, batch, operation = "update")
              },
          )
        }
      }
    } catch (e: BatchOperationException) {
      @Suppress("UNCHECKED_CAST") // We know the entity is EntityT on this repository
      throw mapCreateOrUpdateException(e.cause, e.entity as EntityT)
    }
  }

  override fun batchDelete(entities: Iterable<Versioned<EntityT>>) {
    batchDeleteInternal(BatchProvider.fromIterable(entities))
  }

  override fun batchDelete(entities: Iterator<Versioned<EntityT>>) {
    batchDeleteInternal(BatchProvider.fromIterator(entities))
  }

  private fun batchDeleteInternal(batchProvider: BatchProvider<Versioned<EntityT>>) {
    if (batchProvider.isEmpty()) {
      return
    }

    transactional {
      useHandle { handle ->
        executeBatchOperation(
            handle,
            batchProvider,
            statement =
                """
                  DELETE FROM "${tableName}"
                  WHERE
                    id = :id AND
                    version = :previousVersion
                """
                    .trimIndent(),
            bindParameters = { batch, entity ->
              batch.bind("id", entity.data.id).bind("previousVersion", entity.version)
            },
            handleModifiedRowCounts = { counts, batch ->
              handleModifiedRowCounts(counts, batch, operation = "delete")
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
      bind: Query.() -> Query = { this },
  ): List<Versioned<EntityT>> {
    useHandle { handle ->
      val results =
          getByPredicateInternal(
              handle = handle,
              sqlWhere = sqlWhere,
              limit = limit,
              offset = offset,
              orderBy = orderBy,
              orderDesc = orderDesc,
              nullsFirst = nullsFirst,
              handleJsonNullsInOrderBy = handleJsonNullsInOrderBy,
              forUpdate = forUpdate,
              bind = bind,
          )
      return results.list()
    }
  }

  /**
   * Runs a SELECT query using the given WHERE clause, limit, offset etc., and gives you a stream of
   * results through the given [useStream] argument. The stream must only be used inside the scope
   * of [useStream] - after it returns, the result stream is closed and associated database
   * resources are released (this is done in an exception-safe way, so database resources are never
   * wasted).
   *
   * When using parameters in [sqlWhere], you must remember to bind them through the [bind] function
   * argument (do not concatenate user input directly in [sqlWhere], as that exposes you to SQL
   * injections). When using a list parameter, use the [Query.bindArray] method and pass the class
   * of the list's elements as the second argument (required for JDBI's reflection to work - see
   * example on [getByPredicate]).
   *
   * [sqlWhere] will typically use Postgres JSON operators to filter on entity fields, since the
   * document store uses `jsonb` to store entities (see
   * [Postgres docs](https://www.postgresql.org/docs/16/functions-json.html)).
   *
   * Example implementing a query where we want to stream events of a given type:
   * ```
   * fun streamByEventType(
   *     type: EventType,
   *     // We must take a `useStream` lambda here instead of returning the stream, since the stream
   *     // can only be used in the scope of the lambda passed to `streamByPredicate`.
   *     //
   *     // Place this as the final argument, so users can use trailing lambda syntax, like:
   *     // eventRepo.streamByEventType(EventType.STATUS_UPDATE) { stream -> ... }
   *     useStream: (Stream<Versioned<Event>>) -> Unit,
   * ) {
   *   streamByPredicate(useStream, "data->>'type' = :type") {
   *     bind("type", type.name)
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
   * @return The same as [useStream] (a generic return type based on the passed lambda).
   */
  protected fun <ReturnT> streamByPredicate(
      useStream: (Stream<Versioned<EntityT>>) -> ReturnT,
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      nullsFirst: Boolean = orderDesc,
      handleJsonNullsInOrderBy: Boolean = false,
      forUpdate: Boolean = false,
      bind: Query.() -> Query = { this },
  ): ReturnT {
    // Must wrap the query in a transaction for fetchSize to work
    transactional {
      useHandle { handle ->
        val results =
            getByPredicateInternal(
                handle = handle,
                sqlWhere = sqlWhere,
                limit = limit,
                offset = offset,
                orderBy = orderBy,
                orderDesc = orderDesc,
                nullsFirst = nullsFirst,
                handleJsonNullsInOrderBy = handleJsonNullsInOrderBy,
                forUpdate = forUpdate,
                bind = bind,
                fetchSize = 50,
            )
        return results.stream().use(useStream)
      }
    }
  }

  /**
   * Shared implementation for [getByPredicate] and [streamByPredicate].
   *
   * We want to keep this internal, since the returned [ResultIterable] _must_ be closed in order to
   * not leak database resources. We don't want to expose our users to this pitfall, so we instead
   * expose [getByPredicate]/[streamByPredicate] that ensure that database resources are properly
   * closed. In [getByPredicate], this is done by calling [ResultIterable.list], which iterates over
   * all results and closes the iterable at the end. In [streamByPredicate], we take a lambda
   * argument to use the stream instead of returning the stream itself, to ensure that the stream is
   * only used inside a [use] block which closes the stream at the end of the block.
   */
  @PublishedApi
  internal fun getByPredicateInternal(
      handle: Handle,
      sqlWhere: String,
      limit: Int?,
      offset: Int?,
      orderBy: String?,
      orderDesc: Boolean,
      nullsFirst: Boolean,
      handleJsonNullsInOrderBy: Boolean,
      forUpdate: Boolean,
      bind: Query.() -> Query,
      /**
       * Set this to enable streaming from the database in chunks of the given size. This only works
       * when used inside a transaction (so if you set this, wrap the call to this method in
       * [transactional]).
       */
      fetchSize: Int? = null,
  ): ResultIterable<Versioned<EntityT>> {
    val orderByString: String =
        when {
          orderBy == null -> Columns.CREATED_AT
          handleJsonNullsInOrderBy -> "NULLIF(${orderBy}, 'null')"
          else -> orderBy
        }

    val orderDirection = if (orderDesc) " DESC" else "" // Defaults to ASC
    val orderNulls =
        when {
          // NULLS FIRST is already assumed for ORDER BY DESC
          nullsFirst && !orderDesc -> " NULLS FIRST"
          // NULLS LAST is already assumed for ORDER BY ASC
          !nullsFirst && orderDesc -> " NULLS LAST"
          else -> ""
        }

    val limitString = if (limit != null) "LIMIT ${limit}" else ""
    val offsetString = if (offset != null) "OFFSET ${offset}" else ""
    val forUpdateString = if (forUpdate) " FOR UPDATE" else ""

    return handle
        .createQuery(
            """
              SELECT id, data, version, created_at, modified_at
              FROM "${tableName}"
              WHERE (${sqlWhere})
              ORDER BY ${orderByString}${orderDirection}${orderNulls}
              ${limitString}
              ${offsetString}
              ${forUpdateString}
            """
                .trimIndent(),
        )
        .bind()
        .let { query ->
          if (fetchSize != null) {
            query.setFetchSize(fetchSize)
          } else {
            query
          }
        }
        .map(rowMapper)
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
      bind: Query.() -> Query = { this },
  ): ListWithTotalCount<Versioned<EntityT>> {
    useHandle { handle ->
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
               * Should never happen: the query should always return 1 row with the count, even if
               * the results are empty (see [MappedEntityWithTotalCount]).
               */
              ?: throw IllegalStateException("Failed to get total count of objects in search query")

      return ListWithTotalCount(entities, totalCount)
    }
  }

  override fun countAll(): Long {
    return countByPredicate() // Defaults to all
  }

  /**
   * Runs a `SELECT count(*)` query using the given WHERE clause.
   *
   * When using parameters in [sqlWhere], you must remember to bind them through the [bind] function
   * argument (do not concatenate user input directly in [sqlWhere], as that exposes you to SQL
   * injections). When using a list parameter, use the [Query.bindArray] method and pass the class
   * of the list's elements as the second argument (required for JDBI's reflection to work - see
   * example on [getByPredicate]).
   */
  protected fun countByPredicate(
      sqlWhere: String = "TRUE",
      bind: Query.() -> Query = { this },
  ): Long {
    useHandle { handle ->
      return handle
          .createQuery(
              """
                SELECT count(*)
                FROM "${tableName}"
                WHERE (${sqlWhere})
              """
                  .trimIndent(),
          )
          .bind()
          .map(countMapper)
          .first()
    }
  }

  /**
   * Starts a database transaction, and runs the given [block] inside of it. Calls to other
   * repository methods inside the block will use the same transaction. If an exception is thrown,
   * the transaction is rolled back.
   *
   * The repository's [Jdbi] instance is used for the transaction. If a transaction is already in
   * progress on the current thread, a new one will not be started (since we're already in a
   * transaction).
   *
   * ### Thread safety
   *
   * This function stores a transaction handle in a thread-local, so that operations within [block]
   * can get the handle. But new threads spawned in the scope of `block` will not see this
   * thread-local, and so they will not work correctly with the transaction. So you should not
   * attempt concurrent database operations with this function.
   *
   * ### Mocking
   *
   * See [shouldMockTransactions].
   */
  inline fun <ReturnT> transactional(block: () -> ReturnT): ReturnT {
    // Allows callers to use `block` as if it were in-place
    contract { callsInPlace(block, InvocationKind.EXACTLY_ONCE) }

    if (shouldMockTransactions()) {
      return block()
    }

    return transactional(getJdbiInstance(), block)
  }

  /**
   * Gets a database handle either from an ongoing transaction (from [transactional]), or if none is
   * found, gets a new handle with [Jdbi.open] (automatically closed after the given [block]
   * returns), using the JDBI instance on this repository.
   *
   * You should use this function whenever you want to write custom SQL using Liflig Document Store,
   * so that your implementation plays well with transactions.
   */
  protected inline fun <ReturnT> useHandle(block: (Handle) -> ReturnT): ReturnT {
    return useHandle(jdbi, block)
  }

  /**
   * Method that you can override to map exceptions thrown in [create] / [update] / [batchCreate] /
   * [batchUpdate] to your own exception type. This is useful to handle e.g. unique constraint
   * violations: instead of letting the database throw an opaque `PSQLException` that may be
   * difficult to handle in layers above, you can instead check if the given exception is a unique
   * index violation and map it to a more useful exception type here.
   *
   * If your implementation receives an exception here that it does not want to map, it should just
   * return it as-is.
   *
   * The entity that was attempted to be created or updated is also provided here, so you can add
   * extra context to the mapped exception.
   *
   * This method is only called by [batchCreate] / [batchUpdate] if the batch operation failed
   * because of a single entity (e.g. a unique constraint violation).
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
   * Since [transactional] is an inline method, it cannot be mocked by libraries such as
   * [mockk](https://mockk.io/) that operate on bytecode (since inline functions don't generate
   * bytecode). We still want `transactional` to be inline, to allow non-local returns in the lambda
   * passed to it, which is handy for such scope-based functions.
   *
   * So to support users who want to mock calls to `transactional`, we provide this method, which
   * users can override/mock to return true. When this returns true, `transactional` will just
   * immediately call the lambda passed to it, without starting a database transaction.
   *
   * ### Example
   *
   * ```
   * val mockRepo =
   *     mockk<ExampleRepository> {
   *       every { getOrThrow(exampleId, forUpdate = true) } returns mockEntity
   *       every { update(entity = any(), previousVersion = any()) } returns mockEntity
   *       every { shouldMockTransactions() } returns true
   *     }
   *
   * // Since we set `shouldMockTransactions` to return true above, `transactional` will just
   * // immediately invoke the lambda, without starting a database transaction
   * mockRepo.transactional {
   *   val entity = mockRepo.getOrThrow(exampleId, forUpdate = true)
   *   mockRepo.update(entity.data, entity.version)
   * }
   * ```
   */
  open fun shouldMockTransactions(): Boolean = false

  /**
   * [batchUpdate] and [batchDelete] use optimistic locking: we only update/delete an entity if it
   * matches an expected [Version] - if it does not, then it likely has been concurrently modified
   * in the meantime, in which case we roll back the batch operation and throw a
   * [ConflictRepositoryException]. We check this by going through the modified row counts retrieved
   * in [executeBatchOperation]. This method handles this logic for both of those methods.
   */
  private fun handleModifiedRowCounts(
      modifiedRowCounts: IntArray,
      batch: List<Versioned<EntityT>>,
      operation: String,
  ) {
    for (count in modifiedRowCounts.withIndex()) {
      if (count.value == 0) {
        // Should never be null, but we don't want to suppress the ConflictRepositoryException here
        // if it is
        val conflictedEntity: EntityT? = batch.getOrNull(count.index)?.data
        throw ConflictRepositoryException(
            "Entity was concurrently modified between being retrieved and trying to ${operation} it in batch ${operation} (rolling back batch ${operation}) [Entity: ${conflictedEntity}]",
        )
      }
    }
  }

  /**
   * The inline [transactional] method needs to use the repository's Jdbi instance, but [jdbi] is
   * protected, which doesn't work in public inline functions. So we add this internal helper method
   * to get the Jdbi instance for [transactional]. It needs to be annotated with `@PublishedApi` so
   * we can use it in public inline methods.
   */
  @PublishedApi internal fun getJdbiInstance(): Jdbi = jdbi
}
