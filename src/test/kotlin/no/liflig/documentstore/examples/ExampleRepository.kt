@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.examples

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.EntityList
import no.liflig.documentstore.entity.EntityListWithTotalCount
import no.liflig.documentstore.repository.RepositoryJdbi
import org.jdbi.v3.core.Jdbi

internal enum class OrderBy {
  TEXT,
  CREATED_AT,
}

internal class ExampleRepository(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, ExampleEntity>(
        jdbi,
        tableName = "example",
        ExampleSerializationAdapter,
    ) {
  fun search(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
  ): EntityList<ExampleEntity> {
    return getByPredicate(
        ":text IS NULL OR (data ->>'text' ILIKE '%' || :text || '%')",
        limit = limit,
        offset = offset,
        orderDesc = orderDesc,
        orderBy =
            when (orderBy) {
              OrderBy.TEXT -> "data->>'text'"
              OrderBy.CREATED_AT -> "createdAt"
              null -> null
            },
    ) {
      bind("text", text)
    }
  }

  override fun mapCreateOrUpdateException(e: Exception, entity: ExampleEntity): Exception {
    val message = e.message
    if (message != null && message.contains("example_unique_field_index")) {
      return UniqueFieldAlreadyExists(entity, cause = e)
    }

    return e
  }
}

internal class UniqueFieldAlreadyExists(entity: ExampleEntity, override val cause: Exception) :
    RuntimeException() {
  override val message = "Received entity with unique field that already exists: ${entity}"
}

/**
 * Separate repository, to avoid other tests interfering with the count returned by
 * getByPredicateWithTotalCount.
 */
internal class ExampleRepositoryWithCount(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, ExampleEntity>(
        jdbi,
        tableName = "example_with_count",
        ExampleSerializationAdapter,
    ) {
  fun search(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
  ): EntityListWithTotalCount<ExampleEntity> {
    return getByPredicateWithTotalCount(
        sqlWhere = "(:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))",
        limit = limit,
        offset = offset,
        orderDesc = orderDesc,
        orderBy =
            when (orderBy) {
              OrderBy.TEXT -> "data->>'text'"
              OrderBy.CREATED_AT -> "createdAt"
              null -> null
            },
    ) {
      bind("textQuery", text)
    }
  }
}

internal class ExampleRepositoryWithStringEntityId(jdbi: Jdbi) :
    RepositoryJdbi<ExampleStringId, EntityWithStringId>(
        jdbi,
        tableName = "example_with_string_id",
        EntityWithStringIdSerializationAdapter,
    )
