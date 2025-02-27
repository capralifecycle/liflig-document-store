package no.liflig.documentstore.repository

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import java.text.DecimalFormat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepo
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class BatchTest {
  @BeforeAll
  fun reset() {
    clearDatabase()
  }

  // Pad numbers in test text with 0s so that we can sort by text
  private val testNumberFormat = DecimalFormat("000")
  private val largeBatchSize = 1000

  /**
   * We want to test 3 different input types for the batch operations on Repository:
   * - A List, for the methods that take an Iterable and return results
   * - An Iterator, for the methods that take an Iterator and do not return results
   * - An Iterable created from a Stream, since we want to make sure that the Iterable is only
   *   iterated over once, which is required for types like Stream
   *
   * So to test all batch operations for all these different input types, we write test cases for
   * each input type, and run parameterized tests with these cases for each batch operation.
   */
  abstract class TestCase {
    var entities: List<Versioned<ExampleEntity>>? = null

    abstract val name: String

    override fun toString() = name

    abstract fun batchCreate(entities: List<ExampleEntity>): List<Versioned<ExampleEntity>>

    abstract fun batchUpdate(
        entities: List<Versioned<ExampleEntity>>
    ): List<Versioned<ExampleEntity>>

    abstract fun batchDelete(entities: List<Versioned<ExampleEntity>>)
  }

  object ListTestCase : TestCase() {
    override val name = "List"

    override fun batchCreate(entities: List<ExampleEntity>): List<Versioned<ExampleEntity>> {
      return exampleRepo.batchCreate(entities)
    }

    override fun batchUpdate(
        entities: List<Versioned<ExampleEntity>>
    ): List<Versioned<ExampleEntity>> {
      return exampleRepo.batchUpdate(entities)
    }

    override fun batchDelete(entities: List<Versioned<ExampleEntity>>) {
      exampleRepo.batchDelete(entities)
    }
  }

  object IteratorTestCase : TestCase() {
    override val name = "Iterator"

    override fun batchCreate(entities: List<ExampleEntity>): List<Versioned<ExampleEntity>> {
      exampleRepo.batchCreate(entities.iterator()) shouldBe Unit

      // The Iterator overloads of batch operations don't return results (to avoid allocating a
      // full result list for a potentially large stream), so we have to fetch results here
      // ourselves
      return exampleRepo.listByIds(entities.map { it.id })
    }

    override fun batchUpdate(
        entities: List<Versioned<ExampleEntity>>
    ): List<Versioned<ExampleEntity>> {
      exampleRepo.batchUpdate(entities.iterator()) shouldBe Unit

      return exampleRepo.listByIds(entities.map { it.item.id })
    }

    override fun batchDelete(entities: List<Versioned<ExampleEntity>>) {
      exampleRepo.batchDelete(entities.iterator())
    }
  }

  /** See [TestCase]. */
  object IterableStreamTestCase : TestCase() {
    override val name = "Stream Iterable"

    override fun batchCreate(entities: List<ExampleEntity>): List<Versioned<ExampleEntity>> {
      val stream = entities.stream()
      return exampleRepo.batchCreate(Iterable { stream.iterator() })
    }

    override fun batchUpdate(
        entities: List<Versioned<ExampleEntity>>
    ): List<Versioned<ExampleEntity>> {
      val stream = entities.stream()
      return exampleRepo.batchUpdate(Iterable { stream.iterator() })
    }

    override fun batchDelete(entities: List<Versioned<ExampleEntity>>) {
      val stream = entities.stream()
      return exampleRepo.batchDelete(Iterable { stream.iterator() })
    }
  }

  fun testCases(): List<TestCase> = listOf(ListTestCase, IteratorTestCase, IterableStreamTestCase)

  @ParameterizedTest
  @MethodSource("testCases")
  @Order(1)
  fun `test batchCreate`(test: TestCase) {
    val entitiesToCreate =
        (1..largeBatchSize).map { number ->
          ExampleEntity(text = "batch-test-${testNumberFormat.format(number)}")
        }
    val createdEntities = test.batchCreate(entitiesToCreate)
    test.entities = createdEntities

    assertNotEquals(0, createdEntities.size)
    assertEquals(entitiesToCreate.size, createdEntities.size)
    for (i in entitiesToCreate.indices) {
      assertEquals(entitiesToCreate[i], createdEntities[i].item)
    }

    // Verify that fetching out the created entities gives the same results as the ones we got back
    // from batchCreate
    val fetchedEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    assertEquals(fetchedEntities, createdEntities)
  }

  @ParameterizedTest
  @MethodSource("testCases")
  @Order(2)
  fun `test batchUpdate`(test: TestCase) {
    val createdEntities = test.entities.shouldNotBeNull()

    val entitiesToUpdate =
        createdEntities.withIndex().map { (index, entity) ->
          val updatedEntity =
              entity.item.copy(
                  optionalText = "batch-update-test-${testNumberFormat.format(index + 1)}",
              )
          entity.copy(item = updatedEntity)
        }
    val updatedEntities = test.batchUpdate(entitiesToUpdate)
    test.entities = updatedEntities

    assertEquals(entitiesToUpdate.size, updatedEntities.size)
    for (i in entitiesToUpdate.indices) {
      assertEquals(entitiesToUpdate[i].item, updatedEntities[i].item)
      assertEquals(entitiesToUpdate[i].createdAt, updatedEntities[i].createdAt)
      assertEquals(entitiesToUpdate[i].version.next(), updatedEntities[i].version)
      assertNotEquals(entitiesToUpdate[i].modifiedAt, updatedEntities[i].modifiedAt)
    }

    // Verify that fetching out the updated entities gives the same results as the ones we got back
    // from batchUpdate
    val fetchedEntities = exampleRepo.listByIds(entitiesToUpdate.map { it.item.id })
    assertEquals(fetchedEntities, updatedEntities)
  }

  @ParameterizedTest
  @MethodSource("testCases")
  @Order(3)
  fun `batchUpdate throws ConflictRepositoryException on wrong versions`(test: TestCase) {
    val updatedEntities = test.entities.shouldNotBeNull()

    val entitiesWithWrongVersion =
        updatedEntities.map { it.copy(version = Version(it.version.value - 1)) }

    assertFailsWith<ConflictRepositoryException> { test.batchUpdate(entitiesWithWrongVersion) }
  }

  @ParameterizedTest
  @MethodSource("testCases")
  @Order(4)
  fun `batchDelete throws ConflictRepositoryException on wrong versions`(test: TestCase) {
    val updatedEntities = test.entities.shouldNotBeNull()

    val entitiesWithWrongVersion =
        updatedEntities.map { it.copy(version = Version(it.version.value - 1)) }

    assertFailsWith<ConflictRepositoryException> { test.batchDelete(entitiesWithWrongVersion) }
  }

  @ParameterizedTest
  @MethodSource("testCases")
  @Order(5)
  fun `test batchDelete`(test: TestCase) {
    val entitiesToDelete = test.entities.shouldNotBeNull()

    test.batchDelete(entitiesToDelete)

    val deletedEntities = exampleRepo.listByIds(entitiesToDelete.map { it.item.id })
    deletedEntities.shouldBeEmpty()
  }
}
