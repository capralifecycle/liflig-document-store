package no.liflig.documentstore.dao

import java.util.*
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query

/**
 * A DAO (Data Access Object) for search queries on entities in a database table, where results also
 * include a total count of entities in the table. This is useful for pagination using limit and
 * offset, as the total count can be used to display the number of pages.
 */
interface SearchDaoWithCount<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  fun search(query: SearchQueryT): ListWithTotalCount<VersionedEntity<EntityT>>
  fun listByIds(ids: List<EntityIdT>): List<VersionedEntity<EntityT>>
}

data class ListWithTotalCount<T>(
    val list: List<T>,
    val totalCount: Long,
) {
  /** Maps the elements of the list, while keeping the same [totalCount]. */
  fun <R> map(transform: (T) -> R): ListWithTotalCount<R> {
    return ListWithTotalCount(
        list = this.list.map(transform),
        totalCount = this.totalCount,
    )
  }
}

/**
 * A helper class that you can inherit from to implement a [SearchDaoWithCount], by using the
 * [getByPredicate] method.
 */
abstract class AbstractSearchDaoWithCount<EntityIdT, EntityT, SearchQueryT>(
    protected val jdbi: Jdbi,
    protected val sqlTableName: String,
    protected val serializationAdapter: SerializationAdapter<EntityT>,
) : SearchDaoWithCount<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  private fun fromJson(value: String): EntityT = serializationAdapter.fromJson(value)

  protected open val rowMapperWithCount =
      createRowMapperWithCount(createRowParserWithCount(::fromJson))

  override fun listByIds(ids: List<EntityIdT>): List<VersionedEntity<EntityT>> =
      getByPredicate("id = ANY (:ids)") { bindArray("ids", EntityId::class.java, ids) }.list

  /**
   * Gets database objects matching the given parameters, and the total count of objects matching
   * your WHERE clause without `limit`.
   *
   * This can be used for pagination: for example, if passing e.g. `limit = 10` to display 10 items
   * in a page at a time, the total count can be used to display the number of pages.
   *
   * See [AbstractSearchDao.getByPredicate] for further documentation.
   */
  protected open fun getByPredicate(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      bind: Query.() -> Query = { this }
  ): ListWithTotalCount<VersionedEntity<EntityT>> = mapExceptions {
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
                  // are returned.
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
                      .trimIndent(),
              )
              .bind()
              .map(rowMapperWithCount)
              .list()

      val entities = rows.mapNotNull { row -> row.entity }
      val count =
          rows.firstOrNull()?.count
          // Should never happen: the query should always return 1 row with the count, even if the
          // results are empty (see [EntityRowWithCount])
          ?: throw RuntimeException("Failed to get total count of objects in search query")

      ListWithTotalCount(entities, count)
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