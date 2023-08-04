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
  fun searchWithCount(query: SearchQueryT): Pair<List<VersionedEntity<EntityT>>, Long>
}

/**
 * Extends the [AbstractSearchRepository] from liflig-document-store with methods for getting the
 * count of rows in the database table.
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

  protected open fun getByPredicateWithCount(
    sqlWhere: String = "TRUE",
    limit: Int? = null,
    offset: Int? = null,
    orderBy: String? = null,
    orderDesc: Boolean = false,
    bind: Query.() -> Query = { this }
  ): Pair<List<VersionedEntity<EntityT>>, Long> = mapExceptions {
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
  ): Pair<List<VersionedEntity<EntityT>>, Long> {
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

    val entities = rows.mapNotNull { row -> row.first }
    val count = rows.firstOrNull()?.second ?: throw NoCountReceivedFromSearchQueryException
    return Pair(entities, count)
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

fun <EntityT : EntityRoot<*>> createRowMapperWithCount(
  fromRow: (row: EntityRowWithCount) -> Pair<VersionedEntity<EntityT>?, Long>
): RowMapper<Pair<VersionedEntity<EntityT>?, Long>> {
  val kotlinMapper = KotlinMapper(EntityRowWithCount::class.java)

  return RowMapper { rs, ctx ->
    val simpleRow = kotlinMapper.map(rs, ctx) as EntityRowWithCount
    fromRow(simpleRow)
  }
}

fun <EntityT : EntityRoot<*>> createRowParserWithCount(
  fromJson: (String) -> EntityT
): (row: EntityRowWithCount) -> Pair<VersionedEntity<EntityT>?, Long> {
  return { row ->
    /** @see EntityRowWithCount */
    val entity =
      if (row.data != null && row.version != null)
        VersionedEntity(fromJson(row.data), Version(row.version))
      else null

    Pair(entity, row.count)
  }
}

data object NoCountReceivedFromSearchQueryException : RuntimeException()
