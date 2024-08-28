@file:Suppress("PropertyName") // We want to use the same names here as the database columns

package no.liflig.documentstore.repository

import java.time.Instant
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper

/**
 * A data class that represents fields for a database row that holds an entity instance.
 *
 * Note that the table might include more fields - this is only to read _out_ the entity.
 */
internal data class EntityRow(
    val data: String,
    val version: Long,
    val created_at: Instant,
    val modified_at: Instant,
)

internal fun <EntityT : Entity<*>> createRowMapper(
    parseEntityJson: (String) -> EntityT
): RowMapper<Versioned<EntityT>> {
  val kotlinMapper = KotlinMapper(EntityRow::class.java)

  return RowMapper { resultSet, context ->
    val row = kotlinMapper.map(resultSet, context) as EntityRow
    Versioned(
        parseEntityJson(row.data),
        Version(row.version),
        createdAt = row.created_at,
        modifiedAt = row.modified_at,
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
internal data class UpdateResult(
    val created_at: Instant,
)

internal fun createUpdateResultMapper(): RowMapper<UpdateResult> {
  val kotlinMapper = KotlinMapper(UpdateResult::class.java)

  return RowMapper { resultSet, context -> kotlinMapper.map(resultSet, context) as UpdateResult }
}

/**
 * In the case where no rows are returned, our SQL count query will return a single row where all
 * fields except `count` are `NULL`. Thus, `data` and `version` are nullable here.
 *
 * See [RepositoryJdbi.getByPredicateWithTotalCount].
 */
internal data class EntityRowWithTotalCount(
    val data: String?,
    val version: Long?,
    val created_at: Instant?,
    val modified_at: Instant?,
    val count: Long,
)

internal data class MappedEntityWithTotalCount<EntityT : Entity<*>>(
    val entity: Versioned<EntityT>?,
    val count: Long,
)

internal fun <EntityT : Entity<*>> createRowMapperWithTotalCount(
    parseEntityJson: (String) -> EntityT
): RowMapper<MappedEntityWithTotalCount<EntityT>> {
  val kotlinMapper = KotlinMapper(EntityRowWithTotalCount::class.java)

  return RowMapper { resultSet, context ->
    val row = kotlinMapper.map(resultSet, context) as EntityRowWithTotalCount

    /** @see EntityRowWithTotalCount */
    val entity =
        if (row.data != null &&
            row.version != null &&
            row.created_at != null &&
            row.modified_at != null)
            Versioned(
                parseEntityJson(row.data),
                Version(row.version),
                createdAt = row.created_at,
                modifiedAt = row.modified_at,
            )
        else null

    MappedEntityWithTotalCount(entity, row.count)
  }
}
