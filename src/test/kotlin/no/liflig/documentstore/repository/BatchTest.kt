package no.liflig.documentstore.repository

import java.text.DecimalFormat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.exampleRepo
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
    exampleRepo.batchCreate(entitiesToCreate)

    entities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    assertNotEquals(0, entities.size)
    assertEquals(entitiesToCreate.size, entities.size)
    for ((index, entity) in entities.withIndex()) {
      // We order by text in ExampleRepository.getByTexts, so they should be returned in same order
      assertEquals(entity, entities[index])
    }
  }

  @Order(2)
  @Test
  fun `test batchUpdate`() {
    val updatedEntities =
        entities.withIndex().map { (index, entity) ->
          val updatedEntity =
              entity.item.copy(
                  moreText = "batch-update-test-${testNumberFormat.format(index + 1)}",
              )
          entity.copy(item = updatedEntity)
        }
    exampleRepo.batchUpdate(updatedEntities)

    entities = exampleRepo.listByIds(updatedEntities.map { it.item.id })
    assertEquals(updatedEntities.size, entities.size)
    for ((index, entity) in entities.withIndex()) {
      assertNotNull(entity.item.moreText)
      assertEquals(entity, entities[index])
    }
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
}
