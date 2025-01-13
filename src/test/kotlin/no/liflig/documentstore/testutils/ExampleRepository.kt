@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.testutils

import java.util.stream.Stream
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.ListWithTotalCount
import no.liflig.documentstore.repository.RepositoryJdbi
import no.liflig.documentstore.repository.RepositoryWithGeneratedIds
import org.jdbi.v3.core.Jdbi

enum class OrderBy {
  TEXT,
  OPTIONAL_TEXT,
  CREATED_AT,
}

class ExampleRepository(
    jdbi: Jdbi,
    /**
     * Normally, we would set this directly on the arg to [RepositoryJdbi] below - but here we want
     * to reuse the same repository implementation for different tables, to have separate tables for
     * different tests.
     */
    tableName: String,
) :
    RepositoryJdbi<ExampleId, ExampleEntity>(
        jdbi,
        tableName,
        serializationAdapter = KotlinSerialization(ExampleEntity.serializer()),
    ) {
  fun search(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
      nullsFirst: Boolean = false,
      handleJsonNullsInOrderBy: Boolean = false,
  ): List<Versioned<ExampleEntity>> {
    return getByPredicate(
        ":text IS NULL OR (data->>'text' ILIKE '%' || :text || '%')",
        limit = limit,
        offset = offset,
        orderBy =
            when (orderBy) {
              OrderBy.TEXT -> "data->>'text'"
              OrderBy.OPTIONAL_TEXT -> "data->'optionalText'"
              OrderBy.CREATED_AT -> "created_at"
              null -> null
            },
        orderDesc = orderDesc,
        nullsFirst = nullsFirst,
        handleJsonNullsInOrderBy = handleJsonNullsInOrderBy,
    ) {
      bind("text", text)
    }
  }

  fun searchWithTotalCount(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
      nullsFirst: Boolean = false,
  ): ListWithTotalCount<Versioned<ExampleEntity>> {
    return getByPredicateWithTotalCount(
        "(:textQuery IS NULL OR (data->>'text' ILIKE '%' || :textQuery || '%'))",
        limit = limit,
        offset = offset,
        orderBy =
            when (orderBy) {
              OrderBy.TEXT -> "data->>'text'"
              OrderBy.OPTIONAL_TEXT -> "NULLIF(data->'optionalText', 'null')"
              OrderBy.CREATED_AT -> "created_at"
              null -> null
            },
        orderDesc = orderDesc,
        nullsFirst = nullsFirst,
    ) {
      bind("textQuery", text)
    }
  }

  fun streamingSearch(text: String, useStream: (Stream<Versioned<ExampleEntity>>) -> Unit) {
    return streamByPredicate(useStream, "data->>'text' = :text") { bind("text", text) }
  }

  override fun mapCreateOrUpdateException(e: Exception, entity: ExampleEntity): Exception {
    val message = e.message
    if (message != null && message.contains("example_unique_field_index")) {
      return UniqueFieldAlreadyExists(entity, cause = e)
    }

    return e
  }
}

class UniqueFieldAlreadyExists(
    val failingEntity: ExampleEntity,
    override val cause: Exception,
) : RuntimeException() {
  override val message = "Received entity with unique field that already exists: ${failingEntity}"
}

class ExampleRepositoryWithStringEntityId(jdbi: Jdbi) :
    RepositoryJdbi<ExampleStringId, EntityWithStringId>(
        jdbi,
        tableName = "example_with_string_id",
        serializationAdapter = KotlinSerialization(EntityWithStringId.serializer()),
    )

class ExampleRepositoryWithIntegerEntityId(jdbi: Jdbi) :
    RepositoryJdbi<ExampleIntegerId, EntityWithIntegerId>(
        jdbi,
        tableName = "example_with_integer_id",
        serializationAdapter = KotlinSerialization(EntityWithIntegerId.serializer()),
    )

class ExampleRepositoryWithGeneratedIntegerEntityId(jdbi: Jdbi) :
    RepositoryWithGeneratedIds<ExampleIntegerId, EntityWithIntegerId>(
        jdbi,
        tableName = "example_with_generated_integer_id",
        serializationAdapter = KotlinSerialization(EntityWithIntegerId.serializer()),
    )

class ExampleRepositoryForMigration(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, MigratedExampleEntity>(
        jdbi,
        tableName = MIGRATION_TABLE,
        serializationAdapter = KotlinSerialization(MigratedExampleEntity.serializer()),
    )
