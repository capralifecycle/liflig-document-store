package no.liflig.documentstore.repository

import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper

/**
 * A data class that represents fields for a database row that holds an entity instance.
 *
 * Note that the table might include more fields - this is only to read _out_ the entity.
 */
internal data class EntityRow(val data: String, val version: Long)

internal fun <EntityT : Entity<*>> createRowMapper(
    parseEntityJson: (String) -> EntityT
): RowMapper<VersionedEntity<EntityT>> {
  val kotlinMapper = KotlinMapper(EntityRow::class.java)

  return RowMapper { rs, ctx ->
    val row = kotlinMapper.map(rs, ctx) as EntityRow
    VersionedEntity(parseEntityJson(row.data), Version(row.version))
  }
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
    val count: Long,
)

internal data class MappedEntityWithTotalCount<EntityT : Entity<*>>(
    val entity: VersionedEntity<EntityT>?,
    val count: Long,
)

internal fun <EntityT : Entity<*>> createRowMapperWithTotalCount(
    parseEntityJson: (String) -> EntityT
): RowMapper<MappedEntityWithTotalCount<EntityT>> {
  val kotlinMapper = KotlinMapper(EntityRowWithTotalCount::class.java)

  return RowMapper { rs, ctx ->
    val row = kotlinMapper.map(rs, ctx) as EntityRowWithTotalCount

    /** @see EntityRowWithTotalCount */
    val entity =
        if (row.data != null && row.version != null)
            VersionedEntity(parseEntityJson(row.data), Version(row.version))
        else null

    MappedEntityWithTotalCount(entity, row.count)
  }
}
