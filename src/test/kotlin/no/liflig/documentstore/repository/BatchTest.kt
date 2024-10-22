package no.liflig.documentstore.repository

import java.text.DecimalFormat
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.IntegerEntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.testutils.EntityWithIntegerId
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleIntegerId
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoWithGeneratedIntegerId
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // To keep the same entities field between tests
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class BatchTest {
  // Pad numbers in test text with 0s so that we can sort by text
  private val testNumberFormat = DecimalFormat("000")
  private val largeBatchSize = 1000

  private lateinit var entities: List<Versioned<ExampleEntity>>

  @Order(1)
  @Test
  fun `test batchCreate`() {
    val entitiesToCreate =
        (1..largeBatchSize).map { number ->
          ExampleEntity(text = "batch-test-${testNumberFormat.format(number)}")
        }
    entities = exampleRepo.batchCreate(entitiesToCreate)

    assertNotEquals(0, entities.size)
    assertEquals(entitiesToCreate.size, entities.size)
    for (i in entitiesToCreate.indices) {
      // We order by text in ExampleRepository.getByTexts, so they should be returned in same order
      assertEquals(entitiesToCreate[i], entities[i].item)
    }

    // Verify that fetching out the created entities gives the same results as the ones we got back
    // from batchCreate
    val fetchedEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    assertVersionedEntitiesEqual(fetchedEntities, entities)
  }

  @Order(2)
  @Test
  fun `test batchUpdate`() {
    val entitiesToUpdate =
        entities.withIndex().map { (index, entity) ->
          val updatedEntity =
              entity.item.copy(
                  optionalText = "batch-update-test-${testNumberFormat.format(index + 1)}",
              )
          entity.copy(item = updatedEntity)
        }
    entities = exampleRepo.batchUpdate(entitiesToUpdate)

    assertEquals(entitiesToUpdate.size, entities.size)
    for (i in entitiesToUpdate.indices) {
      assertEquals(entitiesToUpdate[i].item, entities[i].item)
      assertEqualWithinMicrosecond(entitiesToUpdate[i].createdAt, entities[i].createdAt)
      assertNotEquals(entitiesToUpdate[i].version, entities[i].version)
      assertNotEquals(entitiesToUpdate[i].modifiedAt, entities[i].modifiedAt)
    }

    // Verify that fetching out the updated entities gives the same results as the ones we got back
    // from batchUpdate
    val fetchedEntities = exampleRepo.listByIds(entitiesToUpdate.map { it.item.id })
    assertVersionedEntitiesEqual(fetchedEntities, entities)
  }

  @Order(3)
  @Test
  fun `batchUpdate throws ConflictRepositoryException on wrong versions`() {
    val entitiesWithWrongVersion = entities.map { it.copy(version = Version(it.version.value - 1)) }
    assertFailsWith<ConflictRepositoryException> {
      exampleRepo.batchUpdate(entitiesWithWrongVersion)
    }
  }

  @Order(4)
  @Test
  fun `batchDelete throws ConflictRepositoryException on wrong versions`() {
    val entitiesWithWrongVersion = entities.map { it.copy(version = Version(it.version.value - 1)) }
    assertFailsWith<ConflictRepositoryException> {
      exampleRepo.batchDelete(entitiesWithWrongVersion)
    }
  }

  @Order(5)
  @Test
  fun `test batchDelete`() {
    exampleRepo.batchDelete(entities)

    entities = exampleRepo.listByIds(entities.map { it.item.id })
    assertEquals(0, entities.size)
  }

  @Test
  fun `test batchCreate with generated IDs`() {
    val entitiesToCreate =
        (1..largeBatchSize).map { number ->
          EntityWithIntegerId(
              id = ExampleIntegerId(IntegerEntityId.GENERATED),
              text = "batch-test-with-generated-id-${testNumberFormat.format(number)}",
          )
        }
    val createdEntities = exampleRepoWithGeneratedIntegerId.batchCreate(entitiesToCreate)

    assertEquals(entitiesToCreate.size, createdEntities.size)
    // Verify that returned entities are in the same order that we gave them
    for (i in entitiesToCreate.indices) {
      assertEquals(entitiesToCreate[i].text, createdEntities[i].item.text)

      // After calling batchCreate, the IDs should now have been set by the database
      assertNotEquals(IntegerEntityId.GENERATED, createdEntities[i].item.id.value)
    }

    // Verify that fetching out the created entities gives the same results as the ones we got back
    // from batchCreate
    val fetchedEntities =
        exampleRepoWithGeneratedIntegerId.listByIds(createdEntities.map { it.item.id })
    assertVersionedEntitiesEqual(fetchedEntities, createdEntities)
  }
}

/**
 * Postgres stores timestamps with
 * [microsecond precision](https://www.postgresql.org/docs/17/datatype-datetime.html), while Java's
 * `Instant.now()` gives nanosecond precision by default. In some of the asserts we do in these
 * tests, this caused the tests to fail because of differences in precision, although the timestamps
 * really were the same.
 *
 * We may consider changing all calls to `Instant.now()` in the library to truncate to microsecond
 * precision, to match Postgres. But for now, we fix the tests by truncating the timestamps when
 * asserting.
 */
private fun <EntityT : Entity<*>> assertVersionedEntitiesEqual(
    expected: List<Versioned<EntityT>>,
    actual: List<Versioned<EntityT>>
) {
  assertEquals(expected.size, actual.size)

  for (i in expected.indices) {
    assertEquals(expected[i].item, actual[i].item)
    assertEquals(expected[i].version, actual[i].version)
    assertEqualWithinMicrosecond(expected[i].createdAt, actual[i].createdAt)
    assertEqualWithinMicrosecond(expected[i].modifiedAt, actual[i].modifiedAt)
  }
}

private fun assertEqualWithinMicrosecond(expected: Instant, actual: Instant) {
  assert(actual.isAfter(expected.minus(1, ChronoUnit.MICROS)))
  assert(actual.isBefore(expected.plus(1, ChronoUnit.MICROS)))
}
