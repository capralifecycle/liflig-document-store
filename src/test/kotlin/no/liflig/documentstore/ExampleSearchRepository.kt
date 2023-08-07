@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.dao.AbstractSearchRepository
import no.liflig.documentstore.dao.AbstractSearchRepositoryWithCount
import no.liflig.documentstore.dao.EntitiesWithCount
import no.liflig.documentstore.dao.SerializationAdapter
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi

data class ExampleQueryObject(
  val limit: Int? = null,
  val offset: Int? = null,
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
  AbstractSearchRepository<ExampleId, ExampleEntity, ExampleQueryObject>(
    jdbi, sqlTableName, serializationAdapter
  ) {
  override fun listByIds(ids: List<ExampleId>): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  override fun search(query: ExampleQueryObject): List<VersionedEntity<ExampleEntity>> =
    getByPredicate(
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

class ExampleSearchRepositoryWithCount(
  jdbi: Jdbi,
  sqlTableName: String,
  serializationAdapter: SerializationAdapter<ExampleEntity>
) :
  AbstractSearchRepositoryWithCount<ExampleId, ExampleEntity, ExampleQueryObject>(
    jdbi,
    sqlTableName,
    serializationAdapter,
  ) {
  override fun search(query: ExampleQueryObject): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  override fun searchWithCount(
    query: ExampleQueryObject
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
