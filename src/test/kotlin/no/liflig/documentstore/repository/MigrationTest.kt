package no.liflig.documentstore.repository

import java.text.DecimalFormat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoPostMigration
import no.liflig.documentstore.testutils.exampleRepoPreMigration
import no.liflig.documentstore.testutils.examples.ExampleEntity
import no.liflig.documentstore.testutils.jdbi
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

// Use @Order here, so transaction test below does not interfere with main migration test
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class MigrationTest {
  @Order(1)
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

  @Order(2)
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
}
