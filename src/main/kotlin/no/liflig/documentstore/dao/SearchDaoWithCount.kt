package no.liflig.documentstore.dao

import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityList
import no.liflig.documentstore.entity.EntityListWithTotalCount
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import no.liflig.documentstore.entity.getEntityIdType
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query

/**
 * A DAO (Data Access Object) for search queries on entities in a database table, where results also
 * include a total count of entities in the table. This is useful for pagination using limit and
 * offset, as the total count can be used to display the number of pages.
 */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store. Instead of implementing a repository using a SearchDaoWithCount as a field, you should extend RepositoryJdbi and use its getByPredicateWithTotalCount method to implement search/filter.",
    level = DeprecationLevel.WARNING,
)
interface SearchDaoWithCount<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {
  fun search(query: SearchQueryT): EntityListWithTotalCount<EntityT>
  fun listByIds(ids: List<EntityIdT>): EntityList<EntityT>
}

@Deprecated(
    "Package location changed.",
    ReplaceWith(
        "ListWithTotalCount<T>",
        imports = ["no.liflig.documentstore.repository.ListWithTotalCount"],
    ),
    level = DeprecationLevel.WARNING,
)
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
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store. Instead of implementing a repository using a SearchDaoWithCount as a field, you should extend RepositoryJdbi and use its getByPredicateWithTotalCount method to implement search/filter. You can move your search implementation from AbstractSearchDaoWithCount to RepositoryJdbi.",
    level = DeprecationLevel.WARNING,
)
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

  override fun listByIds(ids: List<EntityIdT>): EntityList<EntityT> {
    if (ids.isEmpty()) {
      return emptyList()
    }

    val elementType = getEntityIdType(ids.first())
    return getByPredicate("id = ANY (:ids)") { bindArray("ids", elementType, ids) }.list
  }

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
 * fields except `count` are `NULL`. Thus, `data` and `version` are nullable here.
 */
@Deprecated(
    "In a future version of Liflig Document Store, this will no longer be public. If you rely on this, give a heads-up to the Liflig developers.",
    level = DeprecationLevel.WARNING,
)
data class EntityRowWithCount(
    val data: String?,
    val version: Long?,
    val count: Long,
)

@Deprecated(
    "In a future version of Liflig Document Store, this will no longer be public. If you rely on this, give a heads-up to the Liflig developers.",
    level = DeprecationLevel.WARNING,
)
data class MappedEntityWithCount<EntityT : EntityRoot<*>>(
    val entity: VersionedEntity<EntityT>?,
    val count: Long,
)

@Deprecated(
    "In a future version of Liflig Document Store, this will no longer be public. If you rely on this, give a heads-up to the Liflig developers.",
    level = DeprecationLevel.WARNING,
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

@Deprecated(
    "In a future version of Liflig Document Store, this will no longer be public. If you rely on this, give a heads-up to the Liflig developers.",
    level = DeprecationLevel.WARNING,
)
fun <EntityT : EntityRoot<*>> createRowParserWithCount(
    fromJson: (String) -> EntityT
): (row: EntityRowWithCount) -> MappedEntityWithCount<EntityT> {
  return { row ->
    /** @see EntityRowWithCount */
    val entity =
        if (row.data != null && row.version != null)
            VersionedEntity(fromJson(row.data), Version(row.version))
        else null

    MappedEntityWithCount(entity, row.count)
  }
}
