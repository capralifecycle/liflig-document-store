@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.testexamples

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.dao.AbstractSearchRepository
import no.liflig.documentstore.dao.AbstractSearchRepositoryWithCount
import no.liflig.documentstore.dao.EntitiesWithCount
import no.liflig.documentstore.dao.SearchRepositoryQuery
import no.liflig.documentstore.dao.SerializationAdapter
import no.liflig.documentstore.entity.VersionedEntity
import org.checkerframework.checker.units.qual.A
import org.checkerframework.checker.units.qual.C
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Query

data class ExampleQueryObject<A>(
  val limit: Int? = null,
  val offset: Int? = null,
  val domainPredicate: ((A) -> Boolean)? = null,
  val orderBy: OrderBy? = null,
  val orderDesc: Boolean = false,
)

enum class OrderBy {
  TEXT,
  CREATED_AT,
}

class ExampleSearchRepository(
  jdbi: Jdbi,
  sqlTableName: String,
  serializationAdapter: SerializationAdapter<ExampleEntity>
) :
  AbstractSearchRepository<ExampleId, ExampleEntity, ExampleQueryObject<ExampleEntity>>(
    jdbi, sqlTableName, serializationAdapter
  ) {
  override fun listByIds(ids: List<ExampleId>): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  fun listAll(): List<VersionedEntity<ExampleEntity>> = getByPredicate()

  override fun search(query: ExampleQueryObject<ExampleEntity>): List<VersionedEntity<ExampleEntity>> =
    getByPredicate(
      limit = query.limit,
      offset = query.offset,
      domainPredicate = query.domainPredicate,
      orderDesc = query.orderDesc,
      orderBy =
      when (query.orderBy) {
        OrderBy.TEXT -> "data->>'text'"
        OrderBy.CREATED_AT -> "createdAt"
        null -> null
      }
    )
}

class ExampleSearchRepositoryWithCount(
  jdbi: Jdbi,
  sqlTableName: String,
  serializationAdapter: SerializationAdapter<ExampleEntity>
) :
  AbstractSearchRepositoryWithCount<ExampleId, ExampleEntity, ExampleQueryObject<ExampleEntity>>(
    jdbi,
    sqlTableName,
    serializationAdapter,
  ) {
  override fun search(query: ExampleQueryObject<ExampleEntity>): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  override fun searchWithCount(
    query: ExampleQueryObject<ExampleEntity>
  ): EntitiesWithCount<ExampleEntity> {
    return getByPredicateWithCount(
      limit = query.limit,
      offset = query.offset,
      orderDesc = query.orderDesc,
      orderBy =
      when (query.orderBy) {
        OrderBy.TEXT -> "data->>'text'"
        OrderBy.CREATED_AT -> "createdAt"
        null -> null
      }
    )
  }
}

data class ExampleTextSearchQuery(
  val text: String,
  override val limit: Int? = null,
  override val offset: Int? = null
) : SearchRepositoryQuery() {
  override val sqlWhere: String = "(data->>'text' ILIKE '%' || :text || '%')"

  override val bindSqlParameters: Query.() -> Query = { bind("text", text) }
}
