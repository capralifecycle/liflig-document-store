package no.liflig.documentstore.dao

import java.util.*
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityList
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.Query

/** A DAO (Data Access Object) for search queries on entities in a database table. */
interface SearchDao<EntityIdT : EntityId, EntityT : EntityRoot<EntityIdT>, SearchQueryT> {
  fun search(query: SearchQueryT): EntityList<EntityT>
  fun listByIds(ids: List<EntityIdT>): EntityList<EntityT>
}

/**
 * A helper class that you can inherit from to implement a [SearchDao], by using the
 * [getByPredicate] method (see example on that method's docstring).
 */
abstract class AbstractSearchDao<EntityIdT, EntityT, SearchQueryT>(
    protected val jdbi: Jdbi,
    protected val sqlTableName: String,
    protected val serializationAdapter: SerializationAdapter<EntityT>,
) : SearchDao<EntityIdT, EntityT, SearchQueryT> where
EntityIdT : EntityId,
EntityT : EntityRoot<EntityIdT> {

  private fun fromJson(value: String): EntityT = serializationAdapter.fromJson(value)

  protected open val rowMapper = createRowMapper(createRowParser(::fromJson))

  override fun listByIds(ids: List<EntityIdT>): EntityList<EntityT> =
      getByPredicate("id = ANY (:ids)") { bindArray("ids", EntityId::class.java, ids) }

  /**
   * Runs a SELECT query using the given WHERE clause, limit, offset etc.
   *
   * When using parameters in [sqlWhere], you must remember to bind them through the [bind] function
   * argument (do not concatenate user input directly in [sqlWhere], as that can lead to SQL
   * injections). When using a list parameter, use the [Query.bindArray] method and pass the class
   * of the list's element as the second argument (required for Jdbi's reflection to work - see
   * example below).
   *
   * [sqlWhere] will typically use Postgres JSON operators to filter on entity fields, since the
   * document store uses jsonb to store entities (see Postgres docs:
   * https://www.postgresql.org/docs/16/functions-json.html).
   *
   * Example implementing [SearchDao.search] for a query where we want to look up users from a list
   * of names:
   * ```
   * data class UserSearchQuery(val names: List<String>)
   *
   * override fun search(query: UserSearchQuery): EntityList<User> {
   *     return getByPredicate("data->>'name' = ANY(:namesQuery)") {
   *         bindArray("namesQuery", String::class.java, query.name)
   *     }
   * }
   * ```
   */
  protected open fun getByPredicate(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      forUpdate: Boolean = false,
      bind: Query.() -> Query = { this }
  ): EntityList<EntityT> = mapExceptions {
    getHandle(jdbi) { handle ->
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      val limitString = limit?.let { "LIMIT $it" } ?: ""
      val offsetString = offset?.let { "OFFSET $it" } ?: ""
      val forUpdateString = if (forUpdate) " FOR UPDATE" else ""

      handle
          .select(
              """
                  SELECT id, data, version, created_at, modified_at
                  FROM "$sqlTableName"
                  WHERE ($sqlWhere)
                  ORDER BY $orderByString $orderDirection
                  $limitString
                  $offsetString
                  $forUpdateString
              """
                  .trimIndent(),
          )
          .bind()
          .map(rowMapper)
          .list()
    }
  }

  protected open fun getByPredicateDomainFiltered(
      sqlWhere: String = "TRUE",
      limit: Int? = null,
      offset: Int? = null,
      orderBy: String? = null,
      orderDesc: Boolean = false,
      domainFilter: (EntityT) -> Boolean = { true },
      bind: Query.() -> Query = { this }
  ): EntityList<EntityT> = mapExceptions {
    getHandle(jdbi) { handle ->
      val orderDirection = if (orderDesc) "DESC" else "ASC"
      val orderByString = orderBy ?: "created_at"

      handle
          .select(
              """
              SELECT id, data, version, created_at, modified_at
              FROM "$sqlTableName"
              WHERE ($sqlWhere)
              ORDER BY $orderByString $orderDirection
          """
                  .trimIndent(),
          )
          .bind()
          .map(rowMapper)
          .asSequence()
          .filter { domainFilter(it.item) }
          .run { offset?.let { drop(it) } ?: this }
          .run { limit?.let { take(it) } ?: this }
          .toList()
    }
  }
}

/**
 * A data class that represents fields for a database row that holds an entity instance.
 *
 * Note that the table might include more fields - this is only to read _out_ the entity.
 */
data class EntityRow(val id: UUID, val data: String, val version: Long)

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
  return { row -> VersionedEntity(fromJson(row.data), Version(row.version)) }
}
