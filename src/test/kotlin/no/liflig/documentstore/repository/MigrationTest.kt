@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.repository

import java.text.DecimalFormat
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoPostMigration
import no.liflig.documentstore.testutils.exampleRepoPreMigration
import no.liflig.documentstore.testutils.examples.ExampleEntity
import no.liflig.documentstore.testutils.examples.ExampleId
import no.liflig.documentstore.testutils.examples.InstantSerializer
import no.liflig.documentstore.testutils.examples.KotlinSerialization
import no.liflig.documentstore.testutils.jdbi
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class MigrationTest {
  @AfterEach
  fun clear() {
    useHandle(jdbi) { handle -> handle.createUpdate("TRUNCATE example_for_migration").execute() }
  }

  @Test
  fun `test migration`() {
    // For deterministic sorting
    val numberFormat = DecimalFormat("00000")

    val existingEntities =
        (0 until 10_000)
            .asSequence()
            .map { number -> ExampleEntity(text = "migration-test-${numberFormat.format(number)}") }
            .asIterable()
    exampleRepoPreMigration.batchCreate(existingEntities)

    exampleRepoPostMigration.migrate(
        transformEntity = { (entity, _) -> entity.copy(newFieldAfterMigration = entity.text) },
    )

    var count = 0
    exampleRepoPostMigration.streamAll { entities ->
      for ((index, entity) in entities.withIndex()) {
        assertEquals(
            "migration-test-${numberFormat.format(index)}",
            entity.item.newFieldAfterMigration,
        )
        count++
      }
    }

    assertEquals(10_000, count)
  }

  @Test
  fun `migration rolls back on failed transaction`() {
    val entitiesToCreate = (0 until 10).map { ExampleEntity(text = "Original") }
    exampleRepoPreMigration.batchCreate(entitiesToCreate)

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })

    assertFailsWith<Exception> {
      transactional(jdbi) {
        exampleRepoPostMigration.migrate(
            transformEntity = { (entity, _) -> entity.copy(text = "Migrated") },
        )
        throw Exception("Rolling back transaction")
      }
    }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.item.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
    for (entity in fetchedEntities) {
      assertEquals("Original", entity.item.text)
    }
  }

  // Disabled for automatic tests, since this is a heavy test that we don't want to run in CI
  @Disabled
  @Test
  fun `load-test migration`() {
    val existingEntities =
        (0L until 500_000L)
            .asSequence()
            .map { number ->
              // Create unique values for each field, to make the data more realistic
              LargeEntityPreMigration(
                  field1 = TwoStrings("field1-${number}", "field1-${number +1}"),
                  field2 = StringAndInstant("field2-${number}", Instant.now()),
                  field3 =
                      EnumAndInstant(ExampleEnum.valueOf("VALUE_${number % 4}"), Instant.now()),
                  field4 = listOf("field4-${number}", "field4-${number + 1}"),
                  field5 = number,
                  field6 = number % 2 == 0L,
                  field7 = "field7-${number}",
                  field8 = "field8-${number}",
              )
            }
            .asIterable()
    largeEntityRepoPreMigration.batchCreate(existingEntities)

    // Give some time after batchCreate, to more accurately simulate migration after creation
    Thread.sleep(1000)

    largeEntityRepoPostMigration.migrate()

    println("Successfully migrated!")
  }
}

private val largeEntityRepoPreMigration: LargeEntityRepoPreMigration by lazy {
  LargeEntityRepoPreMigration(jdbi)
}

private class LargeEntityRepoPreMigration(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, LargeEntityPreMigration>(
        jdbi,
        // We can re-use the migration table here, since we truncate it between every test
        tableName = "example_for_migration",
        serializationAdapter = KotlinSerialization(LargeEntityPreMigration.serializer()),
    )

private val largeEntityRepoPostMigration: LargeEntityRepoPostMigration by lazy {
  LargeEntityRepoPostMigration(jdbi)
}

private class LargeEntityRepoPostMigration(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, LargeEntityPostMigration>(
        jdbi,
        // We can re-use the migration table here, since we truncate it between every test
        tableName = "example_for_migration",
        serializationAdapter = KotlinSerialization(LargeEntityPostMigration.serializer()),
    )

/**
 * An example of a larger entity than [ExampleEntity], for load-testing the repository migration API
 * with more realistic data.
 */
@Serializable
private data class LargeEntityPreMigration(
    override val id: ExampleId = ExampleId(),
    val field1: TwoStrings,
    val field2: StringAndInstant,
    val field3: EnumAndInstant,
    val field4: List<String>,
    val field5: Long,
    val field6: Boolean,
    val field7: String,
    val field8: String,
) : Entity<ExampleId>

@Serializable
private data class LargeEntityPostMigration(
    override val id: ExampleId = ExampleId(),
    val field1: TwoStrings,
    val field2: StringAndInstant,
    val field3: EnumAndInstant,
    val field4: List<String>,
    val field5: Long,
    val field6: Boolean,
    val field7: String,
    val field8: String,
    val field9: String? = null,
    val field10: String = field7,
) : Entity<ExampleId>

@Serializable
private data class TwoStrings(
    val field1: String,
    val field2: String,
)

@Serializable
private data class StringAndInstant(
    val field1: String,
    val field2: Instant,
)

@Serializable
private data class EnumAndInstant(
    val field1: ExampleEnum,
    val field2: Instant,
)

@Serializable
enum class ExampleEnum {
  VALUE_0,
  VALUE_1,
  VALUE_2,
  VALUE_3,
}
