package no.liflig.documentstore.dao

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.CloseException
import org.jdbi.v3.core.ConnectionException
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query
import java.io.InterruptedIOException
import java.sql.SQLTransientException
import java.time.Instant
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

// TODO: update docs
/**
 * A Repository holds the logic for persistence of an [EntityRoot],
 * including lookup methods.
 *
 * The [Dao] owns how an [EntityRoot] is serialized, and it is
 * free to make optimizations so that later lookups will be less costly,
 * e.g. by putting some fields from the [EntityRoot] into specific columns
 * in the persisted storage, which can then be indexed and used for lookup
 * by other [Dao] methods.
 *
 * We want all our methods in the [Dao] to return a [Response] type.
 *
 * For this to work properly, all methods should be wrapped using the
 * [mapExceptionsToResponse] method, which takes care of the exceptions
 * and converts them into proper [Either] types. Note we have no way
 * of consistently guarantee this, so it is left for the developer to
 * remember it.
 */
interface Dao

/**
 * A base for a CRUD-like Repository.
 *
 * Note that this does not mean we cannot have more methods, just that we expect
 * these methods for managing persistence of an entity in a consistent way.
 */
interface CrudDao<I : EntityId, A : EntityRoot<I>> : Dao {

  suspend fun create(entity: A): VersionedEntity<A>

  suspend fun getByIdList(ids: List<I>): List<VersionedEntity<A>>

  suspend fun get(id: I): VersionedEntity<A>? =
    getByIdList(listOf(id)).firstOrNull()

  suspend fun <A2 : A> update(
    entity: A2,
    previousVersion: Version,
  ): VersionedEntity<A2>

  suspend fun delete(id: I, previousVersion: Version): Unit
}

/**
 * An abstract Repository to hold common logic we share.
 */
abstract class AbstractCrudDao<I, A>(
  protected val jdbi: Jdbi,
  protected val sqlTableName: String,
  protected val serializationAdapter: SerializationAdapter<A>,
) : CrudDao<I, A>
  where I : EntityId,
        A : EntityRoot<I> {

  private fun toJson(entity: A): String = serializationAdapter.toJson(entity)
  private fun fromJson(value: String): A = serializationAdapter.fromJson(value)

  protected open val rowMapper = createRowMapper(createRowParser(::fromJson))

  /**
   * Extension point to extend the coroutine context used when switching
   * dispatcher, e.g. to add MDC context.
   */
  protected open val coroutineContext: CoroutineContext = EmptyCoroutineContext

  protected open suspend fun getByPredicate(
    sqlWhere: String = "TRUE",
    bind: Query.() -> Query = { this }
  ): List<VersionedEntity<A>> = mapExceptions {
    withContext(Dispatchers.IO + coroutineContext) {
      jdbi.open().use { handle ->
        handle
          .select(
            """
            SELECT id, data, version, created_at, modified_at
            FROM "$sqlTableName"
            WHERE ($sqlWhere)
            ORDER BY created_at
            """.trimIndent()
          )
          .bind()
          .map(rowMapper)
          .list()
      }
    }
  }

  override suspend fun getByIdList(
    ids: List<I>
  ): List<VersionedEntity<A>> =
    getByPredicate("id = ANY (:ids)") {
      bindArray("ids", EntityId::class.java, ids)
    }

  override suspend fun delete(
    id: I,
    previousVersion: Version
  ): Unit = mapExceptions {
    withContext(Dispatchers.IO + coroutineContext) {
      jdbi.open().use { handle ->
        val deleted = handle
          .createUpdate(
            """
            DELETE FROM "$sqlTableName"
            WHERE id = :id AND version = :previousVersion
            """.trimIndent()
          )
          .bind("id", id)
          .bind("previousVersion", previousVersion)
          .execute()

        if (deleted == 0) throw ConflictDaoException()
        else Unit
      }
    }
  }

  /**
   * Default implementation for create. Note that some repositories might need to
   * implement its own version if there are special columns that needs to be
   * kept in sync e.g. for indexing purposes.
   */
  override suspend fun create(
    entity: A
  ): VersionedEntity<A> = mapExceptions {
    withContext(Dispatchers.IO + coroutineContext) {
      VersionedEntity(entity, Version.initial()).also {
        jdbi.open().use { handle ->
          val now = Instant.now()
          handle
            .createUpdate(
              """
              INSERT INTO "$sqlTableName" (id, version, data, modified_at, created_at)
              VALUES (:id, :version, :data::jsonb, :modifiedAt, :createdAt)
              """.trimIndent()
            )
            .bind("id", entity.id)
            .bind("version", it.version)
            .bind("data", toJson(entity))
            .bind("modifiedAt", now)
            .bind("createdAt", now)
            .execute()
        }
      }
    }
  }

  /**
   * Default implementation for update. Note that some repositories might need to
   * implement its own version if there are special columns that needs to be
   * kept in sync e.g. for indexing purposes.
   */
  override suspend fun <A2 : A> update(
    entity: A2,
    previousVersion: Version
  ): VersionedEntity<A2> = mapExceptions {
    withContext(Dispatchers.IO + coroutineContext) {
      jdbi.open().use { handle ->
        val result = VersionedEntity(entity, previousVersion.next())
        val updated =
          handle
            .createUpdate(
              """
              UPDATE "$sqlTableName"
              SET
                version = :nextVersion,
                data = :data::jsonb,
                modified_at = :modifiedAt
              WHERE
                id = :id AND
                version = :previousVersion
              """.trimIndent()
            )
            .bind("nextVersion", result.version)
            .bind("data", toJson(entity))
            .bind("id", entity.id)
            .bind("modifiedAt", Instant.now())
            .bind("previousVersion", previousVersion)
            .execute()

        if (updated == 0) throw ConflictDaoException()
        else result
      }
    }
  }
}

/**
 * A data class that represents fields for a database row that holds an entity instance.
 *
 * Note that the table might include more fields - this is only to read _out_ the entity.
 */
data class EntityRow(
  val id: UUID,
  val data: String,
  val version: Long
)

fun <A : EntityRoot<*>> createRowMapper(
  fromRow: (row: EntityRow) -> VersionedEntity<A>
): RowMapper<VersionedEntity<A>> {
  val kotlinMapper = KotlinMapper(EntityRow::class.java)

  return RowMapper { rs, ctx ->
    val simpleRow = kotlinMapper.map(rs, ctx) as EntityRow
    fromRow(simpleRow)
  }
}

fun <A : EntityRoot<*>> createRowParser(
  fromJson: (String) -> A
): (row: EntityRow) -> VersionedEntity<A> {
  return { row ->
    VersionedEntity(
      fromJson(row.data),
      Version(row.version)
    )
  }
}

/**
 * An exception for an operation on a Repository.
 */
sealed class DaoException : RuntimeException()

class ConflictDaoException() : DaoException()
data class UnavailableDaoException(
  val e: Exception,
) : DaoException()

data class UnknownDaoException(
  val e: Exception,
) : DaoException()

inline fun <T> mapExceptions(block: () -> T): T {
  try {
    return block()
  } catch (e: Exception) {
    when (e) {
      is ConflictDaoException -> throw e
      is SQLTransientException,
      is InterruptedIOException,
      is ConnectionException,
      is CloseException -> throw UnavailableDaoException(e)
      else -> throw UnknownDaoException(e)
    }
  }
}
