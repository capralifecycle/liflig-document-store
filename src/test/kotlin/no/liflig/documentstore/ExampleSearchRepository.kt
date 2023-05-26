@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.dao.AbstractSearchRepository
import no.liflig.documentstore.dao.SerializationAdapter
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi

class ExampleQueryObject

class ExampleSearchRepository(
  jdbi: Jdbi,
  sqlTableName: String,
  serializationAdapter: SerializationAdapter<ExampleEntity>
) : AbstractSearchRepository<ExampleId, ExampleEntity, ExampleQueryObject>(jdbi, sqlTableName, serializationAdapter) {
  enum class OrderBy {
    TEXT,
    CREATED_AT,
  }

  override fun listByIds(ids: List<ExampleId>, handle: Handle?): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  override fun search(query: ExampleQueryObject, handle: Handle?): List<VersionedEntity<ExampleEntity>> =
    TODO("Not yet implemented")

  fun search(limit: Int? = null, offset: Int? = null, orderBy: OrderBy? = null, orderDesc: Boolean = false) =
    getByPredicate(
      limit = limit, offset = offset, orderDesc = orderDesc,
      orderBy = when (orderBy) {
        OrderBy.TEXT -> "data->>'text'"
        OrderBy.CREATED_AT -> "createdAt"
        null -> null
      }
    )
}
