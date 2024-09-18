@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.testutils.examples

import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.ListWithTotalCount
import no.liflig.documentstore.repository.OPTIMAL_BATCH_SIZE
import no.liflig.documentstore.repository.RepositoryJdbi
import no.liflig.documentstore.repository.useHandle
import no.liflig.documentstore.testutils.MIGRATION_TABLE
import org.jdbi.v3.core.Jdbi

enum class OrderBy {
  TEXT,
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
        KotlinSerialization(ExampleEntity.serializer()),
    ) {
  fun search(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
  ): List<Versioned<ExampleEntity>> {
    return getByPredicate(
        ":text IS NULL OR (data->>'text' ILIKE '%' || :text || '%')",
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

  fun searchWithTotalCount(
      text: String? = null,
      limit: Int? = null,
      offset: Int? = null,
      orderBy: OrderBy? = null,
      orderDesc: Boolean = false,
  ): ListWithTotalCount<Versioned<ExampleEntity>> {
    return getByPredicateWithTotalCount(
        "(:textQuery IS NULL OR (data ->>'text' ILIKE '%' || :textQuery || '%'))",
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

  override fun mapCreateOrUpdateException(e: Exception, entity: ExampleEntity): Exception {
    val message = e.message
    if (message != null && message.contains("example_unique_field_index")) {
      return UniqueFieldAlreadyExists(entity, cause = e)
    }

    return e
  }
}

class UniqueFieldAlreadyExists(entity: ExampleEntity, override val cause: Exception) :
    RuntimeException() {
  override val message = "Received entity with unique field that already exists: ${entity}"
}

class ExampleRepositoryWithStringEntityId(jdbi: Jdbi) :
    RepositoryJdbi<ExampleStringId, EntityWithStringId>(
        jdbi,
        tableName = "example_with_string_id",
        KotlinSerialization(EntityWithStringId.serializer()),
    )

class ExampleRepositoryForMigration(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, MigratedExampleEntity>(
        jdbi,
        tableName = MIGRATION_TABLE,
        KotlinSerialization(MigratedExampleEntity.serializer()),
    ) {
  /**
   * We test migration with 10 000 entities. To avoid allocating a list of that size, we instead
   * operate on Iterables to make the tests go faster.
   *
   * We take a lambda to consume the entities here instead of returning the Iterable, since we need
   * to close the database handle after using the entities.
   */
  fun streamAll(consumer: (Iterable<Versioned<MigratedExampleEntity>>) -> Unit) {
    useHandle(jdbi) { handle ->
      val entities =
          handle
              .createQuery(
                  """
                    SELECT id, data, version, created_at, modified_at
                    FROM "${tableName}"
                    ORDER BY data->>'text'
                  """,
              )
              // To avoid reading all into memory at all, which the JDBC Postgres driver does by
              // default
              .setFetchSize(OPTIMAL_BATCH_SIZE)
              .map(rowMapper)

      consumer(entities)
    }
  }
}
