@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.examples

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.InstantSerializer
import no.liflig.documentstore.dao.AbstractSearchDao
import no.liflig.documentstore.dao.AbstractSearchDaoWithCount
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

class ExampleSearchDao(jdbi: Jdbi, table: String, serializer: SerializationAdapter<ExampleEntity>) :
    AbstractSearchDao<ExampleId, ExampleEntity, ExampleSearchQuery>(jdbi, table, serializer) {
  override fun search(query: ExampleSearchQuery): List<VersionedEntity<ExampleEntity>> {
    return getByPredicate(
        sqlWhere = "(:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))",
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

class ExampleSearchDaoWithCount(
    jdbi: Jdbi,
    table: String,
    serializer: SerializationAdapter<ExampleEntity>
) :
    AbstractSearchDaoWithCount<ExampleId, ExampleEntity, ExampleSearchQuery>(
        jdbi,
        table,
        serializer,
    ) {
  override fun search(
      query: ExampleSearchQuery
  ): ListWithTotalCount<VersionedEntity<ExampleEntity>> {
    return getByPredicate(
        sqlWhere = "(:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))",
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
