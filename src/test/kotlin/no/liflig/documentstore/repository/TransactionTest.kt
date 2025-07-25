package no.liflig.documentstore.repository

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import no.liflig.documentstore.ExperimentalDocumentStoreApi
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.mapEntities
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleId
import no.liflig.documentstore.testutils.ExampleRepository
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.jdbi
import no.liflig.documentstore.utils.currentTimeWithMicrosecondPrecision
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.postgresql.util.PSQLException
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
  }

  /**
   * We have 3 functions for running database transactions:
   * - The top-level [transactional] function
   * - The [RepositoryJdbi.transactional] method
   * - The [TransactionManager.transactional] method
   *
   * We want to test that all of these work as expected. So we make a test case for each here, and
   * run every test in this class as a "parameterized test", repeating every test for each
   * transactional function.
   */
  class TransactionTestCase(
      private val name: String,
      val transactional: (block: () -> Unit) -> Unit,
  ) {
    override fun toString() = name
  }

  fun transactionTestCases() =
      listOf(
          TransactionTestCase("Top-level transactional function") { block ->
            transactional(jdbi, block)
          },
          TransactionTestCase("RepositoryJdbi transactional method") { block ->
            transactional(jdbi, block)
          },
          TransactionTestCase("TransactionManager transactional method") { block ->
            TransactionManager(jdbi).transactional(block)
          },
      )

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `complete transaction succeeds`(testCase: TransactionTestCase) {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    testCase.transactional {
      exampleRepo.update(entity1.copy(text = "Two"), version1)
      exampleRepo.update(entity2.copy(text = "Two"), version2)
      exampleRepo.get(entity2.id)
    }

    assertNotEquals(entity1.id, entity2.id)
    assertEquals("Two", exampleRepo.get(entity1.id)!!.data.text)
    assertEquals("Two", exampleRepo.get(entity1.id)!!.data.text)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `failed transaction rolls back`(testCase: TransactionTestCase) {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    assertFailsWith<ConflictRepositoryException> {
      testCase.transactional {
        exampleRepo.update(entity1.copy(text = "Two"), version1)
        exampleRepo.update(entity2.copy(text = "Two"), version2.next())
      }
    }

    assertEquals("One", exampleRepo.get(entity1.id)!!.data.text)
    assertEquals("One", exampleRepo.get(entity2.id)!!.data.text)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `failed transaction rolls back on Throwable too, not just Exception`(
      testCase: TransactionTestCase
  ) {
    val entity = ExampleEntity(text = "Test")

    assertFailsWith<Throwable> {
      testCase.transactional {
        exampleRepo.create(entity)
        throw Throwable()
      }
    }

    exampleRepo.get(entity.id).shouldBeNull()
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `transaction within transaction rolls back as expected`(testCase: TransactionTestCase) {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    try {
      testCase.transactional {
        exampleRepo.update(entity1.copy(text = "Two"), version1)
        testCase.transactional { exampleRepo.update(entity2.copy(text = "Two"), version2) }
        throw ConflictRepositoryException("test")
      }
    } catch (_: ConflictRepositoryException) {}

    assertEquals("One", exampleRepo.get(entity1.id)!!.data.text)
    assertEquals("One", exampleRepo.get(entity2.id)!!.data.text)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `transaction prevents race conditions`(testCase: TransactionTestCase) {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "Text"))

    val threadCount = 100
    val latch = CountDownLatch(threadCount)

    // Without locking, we should be getting ConflictRepositoryException when concurrent processes
    // attempt to update the same row. With locking (using forUpdate = true), each transaction will
    // wait until lock is released before reading.
    val threads =
        (1..threadCount)
            .map { index ->
              thread {
                // Wait for all threads to reach this point simultaneously, so they will all start
                // a transaction at the same time
                latch.countDown()
                latch.await()

                testCase.transactional {
                  val first = exampleRepo.get(entity1.id, forUpdate = true)!!
                  val updated = first.data.copy(text = index.toString())
                  exampleRepo.update(updated, first.version)
                }
              }
            }
            .toList()

    // We want to explicitly do this after the .map, so the threads actually run simultaneously
    for (thread in threads) {
      thread.join()
    }

    val expectedVersion: Long = 101 // Initial version 1 + 100 modifications
    assertEquals(expectedVersion, exampleRepo.get(entity1.id)!!.version.value)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `get returns updated data within transaction`(testCase: TransactionTestCase) {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))

    var updatedEntity: Versioned<ExampleEntity>? = null
    testCase.transactional {
      exampleRepo.update(entity1.copy(text = "Two"), version1)
      updatedEntity = exampleRepo.get(entity1.id)
    }

    assertNotNull(updatedEntity)
    assertEquals("Two", updatedEntity!!.data.text)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `batchCreate rolls back on failed transaction`(testCase: TransactionTestCase) {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "batchCreate transaction test") }

    assertFailsWith<Exception> {
      testCase.transactional {
        exampleRepo.batchCreate(entitiesToCreate)
        throw Exception("Rolling back transaction")
      }
    }

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    assertEquals(0, createdEntities.size)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `batchUpdate rolls back on failed transaction`(testCase: TransactionTestCase) {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "Original") }
    exampleRepo.batchCreate(entitiesToCreate)

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    val updatedEntities = createdEntities.mapEntities { it.copy(text = "Updated") }

    assertFailsWith<Exception> {
      testCase.transactional {
        exampleRepo.batchUpdate(updatedEntities)
        throw Exception("Rolling back transaction")
      }
    }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.data.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
    for (entity in fetchedEntities) {
      assertEquals("Original", entity.data.text)
    }
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `batchDelete rolls back on failed transaction`(testCase: TransactionTestCase) {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "Original") }
    exampleRepo.batchCreate(entitiesToCreate)

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })

    assertFailsWith<Exception> {
      testCase.transactional {
        exampleRepo.batchDelete(createdEntities)
        throw Exception("Rolling back transaction")
      }
    }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.data.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
  }

  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `useHandle uses transaction handle`(testCase: TransactionTestCase) {
    var handleWasInTransaction = false

    testCase.transactional {
      useHandle(jdbi) { handle -> handleWasInTransaction = handle.isInTransaction }
    }

    handleWasInTransaction.shouldBeTrue()
  }

  @Test
  fun `non-local return works in transactional`() {
    val entity = ExampleEntity(text = "Test")

    fun createEntity() {
      // Use all `transactional` variants here, to verify that they are all `inline`
      transactional(jdbi) {
        exampleRepo.transactional {
          TransactionManager(jdbi).transactional {
            exampleRepo.create(entity)
            return // Non-local return - returns from createEntity, outside transactional
          }
        }
      }
    }

    createEntity()

    exampleRepo.get(entity.id).shouldNotBeNull().data.shouldBe(entity)
  }

  @Test
  fun `shouldMockTransactions allows mocking transactional on RepositoryJdbi`() {
    val mockEntity =
        Versioned(
            ExampleEntity(text = "Test"),
            Version.initial(),
            createdAt = currentTimeWithMicrosecondPrecision(),
            modifiedAt = currentTimeWithMicrosecondPrecision(),
        )

    val mockRepo =
        mockk<ExampleRepository> {
          every { getOrThrow(id = any(), forUpdate = any()) } returns mockEntity
          every { update(entity = any(), previousVersion = any()) } returns mockEntity
          every { shouldMockTransactions() } returns true
        }

    val updatedEntity =
        mockRepo.transactional {
          val entity = mockRepo.getOrThrow(mockEntity.data.id, forUpdate = true)
          mockRepo.update(entity.data, entity.version)
        }

    updatedEntity.shouldBe(mockEntity)
  }

  @Test
  fun `shouldMockTransactions allows mocking transactional on TransactionManager`() {
    val mockEntity =
        Versioned(
            ExampleEntity(text = "Test"),
            Version.initial(),
            createdAt = currentTimeWithMicrosecondPrecision(),
            modifiedAt = currentTimeWithMicrosecondPrecision(),
        )

    val mockTransactionManager =
        mockk<TransactionManager> { every { shouldMockTransactions() } returns true }

    val mockRepo =
        mockk<ExampleRepository> {
          every { getOrThrow(id = any(), forUpdate = any()) } returns mockEntity
          every { update(entity = any(), previousVersion = any()) } returns mockEntity
        }

    val updatedEntity =
        mockTransactionManager.transactional {
          val entity = mockRepo.getOrThrow(mockEntity.data.id, forUpdate = true)
          mockRepo.update(entity.data, entity.version)
        }

    updatedEntity.shouldBe(mockEntity)
  }

  @OptIn(ExperimentalDocumentStoreApi::class)
  @Test
  fun `nonTransactional allows opting out of current transaction`() {
    try {
      exampleRepo.transactional {
        exampleRepo.create(ExampleEntity(text = "1"))

        nonTransactional {
          exampleRepo.create(ExampleEntity(text = "2"))
        }

        exampleRepo.create(ExampleEntity(text = "3"))

        throw IllegalStateException()
      }
    } catch (_: IllegalStateException) {}

    val entities = exampleRepo.listAll()
    entities.shouldHaveSize(1)
    entities.first().data.text.shouldBe("2")
  }

  @OptIn(ExperimentalDocumentStoreApi::class)
  @Test
  fun `failing sql command inside nonTransactional does not abort outer transaction`() {
    val exampleId = ExampleId(UUID.fromString("da4887b2-632b-40de-bc1c-88858d43c0d6"))
    val firstEntity = exampleRepo.create(ExampleEntity(exampleId, text = "1"))

    exampleRepo.transactional {
      nonTransactional {
        val e = shouldThrow<UnableToExecuteStatementException> {
          exampleRepo.create(ExampleEntity(exampleId, text = "2"))
        }

        // Error code for duplicate primary key
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        e.cause.shouldBeInstanceOf<PSQLException>().sqlState.shouldBe("23505")
      }

      exampleRepo.create(ExampleEntity(text = "3"))
    }

    val entities = exampleRepo.listAll()
    entities.shouldHaveSize(2)
    entities[0].shouldBe(firstEntity)
    entities[1].data.text.shouldBe("3")
  }
}
