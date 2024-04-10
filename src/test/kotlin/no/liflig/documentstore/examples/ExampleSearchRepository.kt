@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.examples

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.InstantSerializer
import no.liflig.documentstore.dao.AbstractSearchRepository
import no.liflig.documentstore.dao.AbstractSearchRepositoryWithCount
import no.liflig.documentstore.dao.ListWithTotalCount
import no.liflig.documentstore.dao.SerializationAdapter
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi

data class ExampleSearchQuery(
    val text: String? = null,
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
    AbstractSearchRepository<ExampleId, ExampleEntity, ExampleSearchQuery>(
        jdbi,
        sqlTableName,
        serializationAdapter,
    ) {
  override fun listByIds(ids: List<ExampleId>): List<VersionedEntity<ExampleEntity>> {
    TODO("Not yet implemented")
  }

  override fun search(query: ExampleSearchQuery): List<VersionedEntity<ExampleEntity>> =
      getByPredicate(
          sqlWhere =
              """
                (:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))
              """
                  .trimIndent(),
          limit = query.limit,
          offset = query.offset,
          orderDesc = query.orderDesc,
          orderBy =
              when (query.orderBy) {
                OrderBy.TEXT -> "data->>'text'"
                OrderBy.CREATED_AT -> "createdAt"
                null -> null
              },
      ) {
        bind("textQuery", query.text)
      }
}

class ExampleSearchRepositoryWithCount(
    jdbi: Jdbi,
    sqlTableName: String,
    serializationAdapter: SerializationAdapter<ExampleEntity>
) :
    AbstractSearchRepositoryWithCount<ExampleId, ExampleEntity, ExampleSearchQuery>(
        jdbi,
        sqlTableName,
        serializationAdapter,
    ) {
  override fun search(
      query: ExampleSearchQuery
  ): ListWithTotalCount<VersionedEntity<ExampleEntity>> {
    return getByPredicate(
        sqlWhere =
            """
              (:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))
            """
                .trimIndent(),
        limit = query.limit,
        offset = query.offset,
        orderDesc = query.orderDesc,
        orderBy =
            when (query.orderBy) {
              OrderBy.TEXT -> "data->>'text'"
              OrderBy.CREATED_AT -> "createdAt"
              null -> null
            },
    ) {
      bind("textQuery", query.text)
    }
  }
}
