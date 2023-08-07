package no.liflig.documentstore.dao

import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query
import java.util.UUID

interface SearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT> :
  SearchRepository<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  fun searchWithCount(query: SearchQueryT): EntitiesWithCount<EntityT>
}

data class EntitiesWithCount<EntityT : EntityRoot<*>> (
  val entities: List<VersionedEntity<EntityT>>,
  val count: Long,
)

/**
 * Extends [AbstractSearchRepository] with functionality for getting the count of objects in the database table.
 */
abstract class AbstractSearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT>(
  jdbi: Jdbi,
  sqlTableName: String,
  serializationAdapter: SerializationAdapter<EntityT>,
) :
  SearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT>,
  AbstractSearchRepository<EntityIdT, EntityT, SearchQueryT>(
    jdbi, sqlTableName, serializationAdapter
  ) where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  private fun fromJson(value: String): EntityT = serializationAdapter.fromJson(value)

  protected open val rowMapperWithCount =
    createRowMapperWithCount(createRowParserWithCount(::fromJson))

  /**
   * Gets database objects matching the given parameters, and the total count of objects in the database.
   *
   * This can be used for pagination: for example, if passing e.g. `limit = 10` to display 10 items in a page at a time,
   * the count can be used to display the number of pages.
   */
  protected open fun getByPredicateWithCount(
    sqlWhere: String = "TRUE",
    limit: Int? = null,
    offset: Int? = null,
    orderBy: String? = null,
    orderDesc: Boolean = false,
    bind: Query.() -> Query = { this }
  ): EntitiesWithCount<EntityT> = mapExceptions {
    val transaction = transactionHandle.get()

    if (transaction != null) {
      innerGetByPredicateWithCount(sqlWhere, transaction, limit, offset, orderBy, orderDesc, bind)
    } else {
      jdbi.open().use { handle ->
        innerGetByPredicateWithCount(sqlWhere, handle, limit, offset, orderBy, orderDesc, bind)
      }
    }
  }

  private fun innerGetByPredicateWithCount(
    sqlWhere: String,
    handle: Handle,
    limit: Int? = null,
    offset: Int? = null,
    orderBy: String? = null,
    desc: Boolean = false,
    bind: Query.() -> Query = { this }
  ): EntitiesWithCount<EntityT> {
    val limitString = limit?.let { "LIMIT $it" } ?: ""
    val offsetString = offset?.let { "OFFSET $it" } ?: ""
    val orderDirection = if (desc) "DESC" else "ASC"
    val orderByString = orderBy ?: "created_at"

    val rows =
      handle
        .select(
          // SQL query based on https://stackoverflow.com/a/28888696
          // Uses a RIGHT JOIN with the count in order to still get the count when no rows are
          // returned.
          """
              WITH base_query AS (
                  SELECT id, data, version, created_at, modified_at
                  FROM "$sqlTableName"
                  WHERE ($sqlWhere)
              )
              SELECT id, data, version, created_at, modified_at, count
              FROM (
                  TABLE base_query
                  ORDER BY $orderByString $orderDirection
                  $limitString
                  $offsetString
              ) sub_query
              RIGHT JOIN (
                  SELECT count(*) FROM base_query
              ) c(count) ON true
          """.trimIndent()
        )
        .bind()
        .map(rowMapperWithCount)
        .list()

    val entities = rows.mapNotNull { row -> row.entity }
    val count = rows.firstOrNull()?.count ?: throw NoCountReceivedFromSearchQueryException
    return EntitiesWithCount(entities, count)
  }
}

/**
 * In the case where no rows are returned, our SQL count query will return a single row where all
 * fields except `count` are `NULL`. Thus, `id`, `data` and `version` are nullable here.
 */
data class EntityRowWithCount(
  val id: UUID?,
  val data: String?,
  val version: Long?,
  val count: Long,
)

data class MappedEntityWithCount<EntityT : EntityRoot<*>> (
  val entity: VersionedEntity<EntityT>?,
  val count: Long,
)

fun <EntityT : EntityRoot<*>> createRowMapperWithCount(
  fromRow: (row: EntityRowWithCount) -> MappedEntityWithCount<EntityT>
): RowMapper<MappedEntityWithCount<EntityT>> {
  val kotlinMapper = KotlinMapper(EntityRowWithCount::class.java)

  return RowMapper { rs, ctx ->
    val simpleRow = kotlinMapper.map(rs, ctx) as EntityRowWithCount
    fromRow(simpleRow)
  }
}

fun <EntityT : EntityRoot<*>> createRowParserWithCount(
  fromJson: (String) -> EntityT
): (row: EntityRowWithCount) -> MappedEntityWithCount<EntityT> {
  return { row ->
    /** @see EntityRowWithCount */
    val entity =
      if (row.id != null && row.data != null && row.version != null)
        VersionedEntity(fromJson(row.data), Version(row.version))
      else null

    MappedEntityWithCount(entity, row.count)
  }
}

data object NoCountReceivedFromSearchQueryException : RuntimeException()
