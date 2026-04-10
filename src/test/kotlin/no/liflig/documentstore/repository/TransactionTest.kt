package no.liflig.documentstore.repository

import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import java.util.UUID
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
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
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

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
          /**
           * We want Document Store methods to also work with [Jdbi.inTransaction] (see
           * [THREAD_LOCAL_TRANSACTION_HANDLE]).
           */
          TransactionTestCase("Jdbi inTransaction method") { block ->
            jdbi.inTransaction<Unit, Exception> { block() }
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

    assertFailsWith<CustomException> {
      testCase.transactional {
        exampleRepo.batchCreate(entitiesToCreate)
        throw CustomException("Rolling back transaction")
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

    assertFailsWith<CustomException> {
      testCase.transactional {
        exampleRepo.batchUpdate(updatedEntities)
        throw CustomException("Rolling back transaction")
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

    assertFailsWith<CustomException> {
      testCase.transactional {
        exampleRepo.batchDelete(createdEntities)
        throw CustomException("Rolling back transaction")
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

  /**
   * See [THREAD_LOCAL_TRANSACTION_HANDLE]: We want [Jdbi.useHandle] to interoperate with Document
   * Store's [transactional] functions.
   */
  @ParameterizedTest
  @MethodSource("transactionTestCases")
  fun `Jdbi useHandle uses transaction handle from document store`(testCase: TransactionTestCase) {
    var handleWasInTransaction = false
    val entityId = "a7f6fefa-9942-44c5-bbf7-0ad3d57d817b"

    assertFailsWith<CustomException> {
      testCase.transactional {
        jdbi.useHandle<Exception> { handle ->
          handleWasInTransaction = handle.isInTransaction

          /**
           * Test manually inserting an entity using this handle, to verify that it rolls back when
           * the transaction fails.
           */
          handle.execute(
              """
              insert into example(
                  id, created_at, modified_at, version, data
              ) values (
                  '${entityId}',
                  '2026-04-10T12:08:48Z',
                  '2026-04-10T12:08:48Z',
                  '1',
                  '{"id":"${entityId}","text":"test"}'
              )
              """
                  .trimIndent()
          )
        }

        throw CustomException("Rolling back transaction")
      }
    }

    handleWasInTransaction.shouldBeTrue()

    /** Manual insert should have been rolled back. */
    exampleRepo.get(ExampleId(UUID.fromString(entityId))).shouldBeNull()
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

  @Test
  fun `lambda uses EXACTLY_ONCE contract in transactional top-level function`() {
    val uninitialized: String

    transactional(jdbi) { uninitialized = "Initialized" }

    // This won't compile unless `transactional` uses `callsInPlace` contract with
    // `InvocationKind.EXACTLY_ONCE`
    useString(uninitialized)
  }

  @Test
  fun `lambda uses EXACTLY_ONCE contract in transactional method on RepositoryJdbi`() {
    val uninitialized: String

    exampleRepo.transactional { uninitialized = "Initialized" }

    // This won't compile unless `transactional` uses `callsInPlace` contract with
    // `InvocationKind.EXACTLY_ONCE`
    useString(uninitialized)
  }

  @Test
  fun `lambda uses EXACTLY_ONCE contract in transactional method on TransactionManager`() {
    val uninitialized: String

    TransactionManager(jdbi).transactional { uninitialized = "Initialized" }

    // This won't compile unless `transactional` uses `callsInPlace` contract with
    // `InvocationKind.EXACTLY_ONCE`
    useString(uninitialized)
  }

  // Dummy method for contract tests
  private fun useString(string: String): Int {
    return string.length
  }
}

/** Custom exception class, to test that a specific exception is thrown. */
private class CustomException(message: String) : Exception(message)
