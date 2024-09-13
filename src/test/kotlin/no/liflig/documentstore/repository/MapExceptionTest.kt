package no.liflig.documentstore.repository

import kotlin.test.assertFailsWith
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.examples.ExampleEntity
import no.liflig.documentstore.testutils.examples.ExampleRepository
import no.liflig.documentstore.testutils.examples.UniqueFieldAlreadyExists
import org.junit.jupiter.api.Test

class MapExceptionTest {
  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in create`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    assertFailsWith<UniqueFieldAlreadyExists> {
      exampleRepo.create(ExampleEntity(text = "A", uniqueField = 1))
      exampleRepo.create(ExampleEntity(text = "B", uniqueField = 1))
    }
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in update`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    assertFailsWith<UniqueFieldAlreadyExists> {
      val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "A", uniqueField = 2))
      exampleRepo.create(ExampleEntity(text = "B", uniqueField = 3))

      exampleRepo.update(entity1.copy(uniqueField = 3), version1)
    }
  }
}
