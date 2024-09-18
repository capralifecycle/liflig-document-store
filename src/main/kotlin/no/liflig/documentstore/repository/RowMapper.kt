@file:Suppress("PropertyName") // We want to use the same names here as the database columns

package no.liflig.documentstore.repository

import java.sql.ResultSet
import java.time.Instant
import java.time.OffsetDateTime
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext

internal object Columns {
  const val DATA = "data"
  const val VERSION = "version"
  const val CREATED_AT = "created_at"
  const val MODIFIED_AT = "modified_at"

  const val COUNT = "count"
}

internal class EntityRowMapper<EntityT : Entity<*>>(
    private val serializationAdapter: SerializationAdapter<EntityT>,
) : RowMapper<Versioned<EntityT>> {
  override fun map(resultSet: ResultSet, ctx: StatementContext): Versioned<EntityT> {
    return mapEntity(resultSet)
  }

  /**
   * Separate from [map], so we can use this in [no.liflig.documentstore.migration.migrateEntity]
   * without having to provide the JDBI-specific [StatementContext].
   */
  fun mapEntity(resultSet: ResultSet): Versioned<EntityT> {
    val data = getStringFromRowOrThrow(resultSet, Columns.DATA)
    val version = getLongFromRowOrThrow(resultSet, Columns.VERSION)
    val createdAt = getInstantFromRowOrThrow(resultSet, Columns.CREATED_AT)
    val modifiedAt = getInstantFromRowOrThrow(resultSet, Columns.MODIFIED_AT)

    return Versioned(
        item = serializationAdapter.fromJson(data),
        version = Version(version),
        createdAt = createdAt,
        modifiedAt = modifiedAt,
    )
  }
}

/**
 * In [RepositoryJdbi.update], we receive an entity and its previous [Version], and want to return a
 * [Versioned] wrapper around that entity. We know the [Versioned.modifiedAt] field, since that is
 * set to `Instant.now()` when updating, but the [Versioned.createdAt] we need to get ourselves.
 *
 * We do this by using the [RETURNING clause](https://www.postgresql.org/docs/16/dml-returning.html)
 * in the SQL query, to return data from our UPDATE statement. Since we only need the `created_at`
 * column, we only return that.
 */
@JvmInline internal value class UpdateResult(val createdAt: Instant)

internal class UpdateResultMapper : RowMapper<UpdateResult> {
  override fun map(resultSet: ResultSet, ctx: StatementContext): UpdateResult {
    val createdAt = getInstantFromRowOrThrow(resultSet, Columns.CREATED_AT)

    return UpdateResult(createdAt)
  }
}

/**
 * In the case where no rows are returned, our SQL count query will return a single row where all
 * fields except `count` are `NULL`. Thus, `entity` is nullable here.
 *
 * See [RepositoryJdbi.getByPredicateWithTotalCount].
 */
internal data class MappedEntityWithTotalCount<EntityT : Entity<*>>(
    val entity: Versioned<EntityT>?,
    val totalCount: Long,
)

internal class EntityRowMapperWithTotalCount<EntityT : Entity<*>>(
    private val serializationAdapter: SerializationAdapter<EntityT>,
) : RowMapper<MappedEntityWithTotalCount<EntityT>> {
  override fun map(
      resultSet: ResultSet,
      ctx: StatementContext
  ): MappedEntityWithTotalCount<EntityT> {
    val totalCount = getLongFromRowOrThrow(resultSet, Columns.COUNT)

    val data = getStringFromRow(resultSet, Columns.DATA)
    val version = getLongFromRow(resultSet, Columns.VERSION)
    val createdAt = getInstantFromRow(resultSet, Columns.CREATED_AT)
    val modifiedAt = getInstantFromRow(resultSet, Columns.MODIFIED_AT)

    val entity =
        if (data != null && version != null && createdAt != null && modifiedAt != null) {
          Versioned(
              item = serializationAdapter.fromJson(data),
              version = Version(version),
              createdAt = createdAt,
              modifiedAt = modifiedAt,
          )
        } else {
          null
        }

    return MappedEntityWithTotalCount(entity, totalCount = totalCount)
  }
}

/**
 * The JDBC driver for Postgres
 * [does not currently support Instant](https://jdbc.postgresql.org/documentation/query/#using-java-8-date-and-time-classes),
 * so we have to go through the [OffsetDateTime] type (which is the type recommended by pgjdbc for
 * `TIMESTAMP WITH TIME ZONE`, a.k.a. `timestamptz`).
 */
private fun getInstantFromRow(resultSet: ResultSet, columnName: String): Instant? {
  val createdAt: OffsetDateTime? = resultSet.getObject(columnName, OffsetDateTime::class.java)
  if (resultSet.wasNull()) { // Checks if the last read column was NULL
    return null
  }
  return createdAt?.toInstant()
}

private fun getInstantFromRowOrThrow(resultSet: ResultSet, columnName: String): Instant {
  return getInstantFromRow(resultSet, columnName)
      ?: throw NullPointerException("Database column '${columnName}' was NULL")
}

/**
 * [ResultSet.getLong] returns 0 if the column was NULL. To distinguish between a valid 0 and NULL,
 * we must call [ResultSet.wasNull]. This function encapsulates that logic.
 */
private fun getLongFromRow(resultSet: ResultSet, columnName: String): Long? {
  val value: Long = resultSet.getLong(columnName)
  if (resultSet.wasNull()) {
    return null
  }
  return value
}

private fun getLongFromRowOrThrow(resultSet: ResultSet, columnName: String): Long {
  return getLongFromRow(resultSet, columnName)
      ?: throw NullPointerException("Database column '${columnName}' was NULL")
}

private fun getStringFromRow(resultSet: ResultSet, columnName: String): String? {
  return resultSet.getString(columnName)
}

private fun getStringFromRowOrThrow(resultSet: ResultSet, columnName: String): String {
  return resultSet.getString(columnName)
      ?: throw NullPointerException("Database column '${columnName}' was NULL")
}
