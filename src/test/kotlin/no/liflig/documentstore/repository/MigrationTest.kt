package no.liflig.documentstore.repository

import java.text.DecimalFormat
import kotlin.test.assertEquals
import no.liflig.documentstore.testutils.exampleRepoPostMigration
import no.liflig.documentstore.testutils.exampleRepoPreMigration
import no.liflig.documentstore.testutils.examples.ExampleEntity
import org.junit.jupiter.api.Test

class MigrationTest {
  @Test
  fun `test migration`() {
    // For deterministic sorting
    val numberFormat = DecimalFormat("00000")

    val entitiesToCreate =
        (0 until 10_000)
            .asSequence()
            .map { number -> ExampleEntity(text = "migration-test-${numberFormat.format(number)}") }
            .asIterable()
    exampleRepoPreMigration.batchCreate(entitiesToCreate)

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
}
