package no.liflig.documentstore.repository

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import no.liflig.documentstore.entity.mapEntities
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleRepository
import no.liflig.documentstore.testutils.UniqueFieldAlreadyExists
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepo
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MapExceptionTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in create`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    val exception =
        assertFailsWith<UniqueFieldAlreadyExists> {
          exampleRepo.create(ExampleEntity(text = "A", uniqueField = 1))
          exampleRepo.create(ExampleEntity(text = "B", uniqueField = 1))
        }
    assertEquals(1, exception.failingEntity.uniqueField)
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in update`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    val exception =
        assertFailsWith<UniqueFieldAlreadyExists> {
          val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "A", uniqueField = 2))
          exampleRepo.create(ExampleEntity(text = "B", uniqueField = 3))

          exampleRepo.update(entity1.copy(uniqueField = 3), version1)
        }
    assertEquals(3, exception.failingEntity.uniqueField)
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in batchCreate`() {
    /**
     * If this test starts failing, that may be because the Postgres JDBC driver has changed the
     * format of their BatchUpdateException, which we use in
     * [no.liflig.documentstore.utils.getFailingEntity] to get the entity that failed.
     */
    val exception =
        assertFailsWith<UniqueFieldAlreadyExists> {
          exampleRepo.batchCreate(
              // Create 100 entities, and an additional entity with the same unique field as the
              // last one, which should cause UniqueFieldAlreadyExists exception from the
              // overridden mapCreateOrUpdateException in the repository
              (100..199).map { i -> ExampleEntity(text = "test", uniqueField = i) } +
                  listOf(ExampleEntity(text = "test", uniqueField = 199)),
          )
        }
    assertEquals(199, exception.failingEntity.uniqueField)
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in batchUpdate`() {
    val entities =
        exampleRepo.batchCreate(
            // Test with 75 entities, to verify that getFailingEntity works with 2-digit indexes
            (200..275).map { i -> ExampleEntity(text = "test", uniqueField = i) },
        )

    /**
     * If this test starts failing, that may be because the Postgres JDBC driver has changed the
     * format of their BatchUpdateException, which we use in
     * [no.liflig.documentstore.utils.getFailingEntity] to get the entity that failed.
     */
    val exception =
        assertFailsWith<UniqueFieldAlreadyExists> {
          exampleRepo.batchUpdate(
              entities.mapEntities { entity ->
                entity.copy(
                    text = "updated",
                    // Update the 100 entities we created, but set the uniqueField on the last one
                    // to the same as a previous entity
                    uniqueField = if (entity.uniqueField == 275) 274 else entity.uniqueField,
                )
              },
          )
        }
    assertEquals(274, exception.failingEntity.uniqueField)
  }
}
