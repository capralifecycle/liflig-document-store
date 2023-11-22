package no.liflig.documentstore.dao

import java.util.UUID
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query

interface SearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT> :
    SearchRepository<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  fun searchWithCount(query: SearchQueryT): EntitiesWithCount<EntityT>
}

data class EntitiesWithCount<EntityT : EntityRoot<*>>(
    val entities: List<VersionedEntity<EntityT>>,
    val count: Long,
)

/**
 * Extends [AbstractSearchRepository] with functionality for getting the count of objects in the
 * database table.
 */
abstract class AbstractSearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT>(
    jdbi: Jdbi,
    sqlTableName: String,
    serializationAdapter: SerializationAdapter<EntityT>,
) :
    SearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT>,
    AbstractSearchRepository<EntityIdT, EntityT, SearchQueryT>(
        jdbi, sqlTableName, serializationAdapter) where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  private fun fromJson(value: String): EntityT = serializationAdapter.fromJson(value)

  protected open val rowMapperWithCount =
      createRowMapperWithCount(createRowParserWithCount(::fromJson))

  /**
   * Gets database objects matching the given parameters, and the total count of objects in the
   * database.
   *
   * This can be used for pagination: for example, if passing e.g. `limit = 10` to display 10 items
   * in a page at a time, the count can be used to display the number of pages.
   */
  protected open fun getByPredicateWithCount(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      bind: Query.() -> Query = { this }
  ): EntitiesWithCount<EntityT> = mapExceptions {
    getHandle(jdbi) { handle ->
      val limitString = limit?.let { "LIMIT $it" } ?: ""
      val offsetString = offset?.let { "OFFSET $it" } ?: ""
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      val rows =
          handle
              .select(
                  // SQL query based on https://stackoverflow.com/a/28888696
                  // Uses a RIGHT JOIN with the count in order to still get the count when no rows
                  // are
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
            """
                      .trimIndent())
              .bind()
              .map(rowMapperWithCount)
              .list()

      val entities = rows.mapNotNull { row -> row.entity }
      val count = rows.firstOrNull()?.count ?: throw NoCountReceivedFromSearchQueryException

      EntitiesWithCount(entities, count)
    }
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

data class MappedEntityWithCount<EntityT : EntityRoot<*>>(
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

/**
 * Extend this class with your custom queries to use [SearchRepositoryWithCountJdbi]. Override
 * fields to customize the query. Use a sealed class to support multiple different query types.
 *
 * NB: if using `sqlWhere`, remember to bind your parameters with `bindSqlParameters`!
 *
 * Example for an entity with a `text` field:
 * ```
 * data class ExampleSearchQuery(
 *   val text: String,
 *   override val limit: Int,
 *   override val offset: Int,
 * ) : SearchRepositoryQuery() {
 *   override val sqlWhere: String = "(data->>'text' ILIKE '%' || :text || '%')"
 *
 *   override val bindSqlParameters: Query.() -> Query = { bind("text", text) }
 * }
 *
 * fun main() {
 *   ...
 *   val searchRepo = SearchRepositoryWithCountJdbi<ExampleId, ExampleEntity, ExampleSearchQuery>(
 *     jdbi, "example", exampleSerializationAdapter,
 *   )
 *
 *   val (entities, count) = searchRepo.searchWithCount(
 *     ExampleTextSearchQuery(text = "Example text", limit = 10, offset = 0)
 *   )
 * }
 * ```
 */
open class SearchRepositoryQuery {
  open val sqlWhere: String = "TRUE"
  open val bindSqlParameters: Query.() -> Query = { this } // Default no-op
  open val limit: Int? = null
  open val offset: Int? = null
  open val orderBy: String? = null
  open val orderDesc: Boolean = false
}

/**
 * Generic implementation of [AbstractSearchRepositoryWithCount] for queries that extend
 * [SearchRepositoryQuery].
 */
open class SearchRepositoryWithCountJdbi<EntityIdT, EntityT, SearchQueryT>(
    jdbi: Jdbi,
    sqlTableName: String,
    serializationAdapter: SerializationAdapter<EntityT>
) :
    AbstractSearchRepositoryWithCount<EntityIdT, EntityT, SearchQueryT>(
        jdbi,
        sqlTableName,
        serializationAdapter,
    ) where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT>,
SearchQueryT : SearchRepositoryQuery {
  override fun search(query: SearchQueryT): List<VersionedEntity<EntityT>> {
    return getByPredicate(
        sqlWhere = query.sqlWhere,
        limit = query.limit,
        offset = query.offset,
        orderBy = query.orderBy,
        orderDesc = query.orderDesc,
        bind = query.bindSqlParameters,
    )
  }

  override fun searchWithCount(query: SearchQueryT): EntitiesWithCount<EntityT> {
    return getByPredicateWithCount(
        sqlWhere = query.sqlWhere,
        limit = query.limit,
        offset = query.offset,
        orderBy = query.orderBy,
        orderDesc = query.orderDesc,
        bind = query.bindSqlParameters,
    )
  }
}
