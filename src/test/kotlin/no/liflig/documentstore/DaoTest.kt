package no.liflig.documentstore

import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeLessThan
import java.time.Instant
import java.util.*
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.ConflictDaoException
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.coTransactional
import no.liflig.documentstore.dao.transactional
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.examples.ExampleEntity
import no.liflig.documentstore.examples.ExampleId
import no.liflig.documentstore.examples.ExampleSearchDao
import no.liflig.documentstore.examples.ExampleSearchDaoWithCount
import no.liflig.documentstore.examples.ExampleSearchQuery
import no.liflig.documentstore.examples.ExampleSerializationAdapter
import no.liflig.documentstore.examples.OrderBy
import no.liflig.snapshot.verifyJsonSnapshot
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Named
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

typealias Transactional = suspend (jdbi: Jdbi, block: suspend () -> Any?) -> Any?

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DaoTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val crudDao = CrudDaoJdbi(jdbi, "example", serializationAdapter)
  val searchDao = ExampleSearchDao(jdbi, "example", serializationAdapter)

  // Separate DAOs to avoid other tests interfering with the count returned by SearchDaoWithCount
  val crudDaoWithCount = CrudDaoJdbi(jdbi, "example_with_count", serializationAdapter)
  val searchDaoWithCount =
      ExampleSearchDaoWithCount(jdbi, "example_with_count", serializationAdapter)

  private fun getTransactionFunctions(): Stream<Arguments> {
    val co: Transactional = ::coTransactional
    val normal: Transactional = { a, b -> transactional(a) { runBlocking { b() } } }
    return Stream.of(
        Arguments.of(Named.of("Non-suspending", normal)),
        Arguments.of(Named.of("Suspending", co)),
    )
  }

  @Test
  fun storeAndRetrieveNewEntity() {
    runBlocking {
      val agg = ExampleEntity(text = "hello world")

      crudDao.create(agg)

      val read = crudDao.get(agg.id)

      assertNotNull(read)
      assertEquals(Version.initial(), read.version)
      assertEquals(agg, read.item)
    }
  }

  @Test
  fun updateWithWrongVersion() {
    runBlocking {
      val agg = ExampleEntity(text = "hello world")

      val storeResult = crudDao.create(agg)

      assertFailsWith<ConflictDaoException> { crudDao.update(agg, storeResult.version.next()) }
    }
  }

  @Test
  fun deleteEntity() {
    runBlocking {
      val agg = ExampleEntity(text = "hello world")

      assertFailsWith<ConflictDaoException> { crudDao.delete(agg.id, Version.initial()) }

      val res2 = crudDao.create(agg)
      assertEquals(Version.initial(), res2.version)

      val res3 = crudDao.delete(agg.id, Version.initial())
      assertEquals(Unit, res3)

      val res4 = crudDao.get(agg.id)
      assertNull(res4)
    }
  }

  @Test
  fun updateEntity() {
    runBlocking {
      val (initialAgg, initialVersion) = crudDao.create(ExampleEntity(text = "hello world"))

      val updatedAgg = initialAgg.update(text = "new value")
      crudDao.update(updatedAgg, initialVersion)

      val res = crudDao.get(updatedAgg.id)

      assertNotNull(res)
      val (agg, version) = res

      assertEquals("new value", agg.text)
      assertNotEquals(initialVersion, version)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun completeTransactionSucceeds(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = "One"))
      val (initialAgg2, initialVersion2) = crudDao.create(ExampleEntity(text = "One"))

      transactionBlock(jdbi) {
        crudDao.update(initialAgg1.update(text = "Two"), initialVersion1)
        crudDao.update(initialAgg2.update(text = "Two"), initialVersion2)
        crudDao.get(initialAgg2.id)
      }

      assertNotEquals(initialAgg1.id, initialAgg2.id)
      assertEquals("Two", crudDao.get(initialAgg1.id)!!.item.text)
      assertEquals("Two", crudDao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest()
  @MethodSource("getTransactionFunctions")
  fun failedTransactionRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = "One"))
      val (initialAgg2, initialVersion2) = crudDao.create(ExampleEntity(text = "One"))

      try {
        transactionBlock(jdbi) {
          crudDao.update(initialAgg1.update(text = "Two"), initialVersion1)
          crudDao.update(initialAgg2.update(text = "Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {}

      assertEquals("One", crudDao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", crudDao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun failedTransactionWithExplicitHandleStartedOutsideRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = "One"))
      val (initialAgg2, initialVersion2) = crudDao.create(ExampleEntity(text = "One"))

      var exceptionThrown = false
      try {
        transactionBlock(jdbi) {
          runBlocking {
            crudDao.update(initialAgg1.update(text = "Two"), initialVersion1)
            crudDao.update(initialAgg2.update(text = "Two"), initialVersion2.next())
          }
        }
      } catch (_: ConflictDaoException) {
        exceptionThrown = true
      }

      assert(exceptionThrown)
      assertEquals("One", crudDao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", crudDao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun failedTransactionFactoryRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = "One"))
      val (initialAgg2, initialVersion2) = crudDao.create(ExampleEntity(text = "One"))

      try {
        transactionBlock(jdbi) {
          crudDao.update(initialAgg1.update(text = "Two"), initialVersion1)
          crudDao.update(initialAgg2.update(text = "Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {}

      assertEquals("One", crudDao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", crudDao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun transactionWithinTransactionRollsBackAsExpected(transactionBlock: Transactional) {
    runBlocking {
      val initialValue = "Initial"
      val updatedVaue = "Updated value"
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = initialValue))
      val (initialAgg2, initialVersion2) = crudDao.create(ExampleEntity(text = initialValue))

      try {
        transactionBlock(jdbi) {
          crudDao.update(initialAgg1.update(text = updatedVaue), initialVersion1)
          transactionBlock(jdbi) {
            crudDao.update(initialAgg2.update(text = updatedVaue), initialVersion2)
          }
          throw ConflictDaoException()
        }
      } catch (_: ConflictDaoException) {}

      assertEquals(initialValue, crudDao.get(initialAgg1.id)!!.item.text)
      assertEquals(initialValue, crudDao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun transactionWithForUpdateLocksRows(transactionBlock: Transactional) {
    runBlocking {
      val initialValue = "Initial"
      val (initialAgg1, _) = crudDao.create(ExampleEntity(text = initialValue))

      // Without locking, we should be getting ConflictDaoException when concurrent processes
      // attempt to update the same row. With locking, each transaction will wait until lock
      // is released before reading
      (1 until 100)
          .map {
            async(Dispatchers.IO) {
              transactionBlock(jdbi) {
                val first = crudDao.get(initialAgg1.id, true)!!
                val updated = first.item.update(text = it.toString())
                crudDao.update(updated, first.version)
              }
            }
          }
          .awaitAll()

      assertEquals(100, crudDao.get(initialAgg1.id)!!.version.value)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun getReturnsUpdatedDataWithinTransaction(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = crudDao.create(ExampleEntity(text = "One"))

      val result =
          transactionBlock(jdbi) {
            crudDao.update(initialAgg1.update(text = "Two"), initialVersion1)
            crudDao.get(initialAgg1.id)?.item?.text
          }

      assertEquals("Two", result)
    }
  }

  @Test
  fun searchDaoTextQuery() {
    runBlocking {
      val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
      crudDao.create(entity1)
      val entity2 = ExampleEntity(text = "Very specific name for text query test 2")
      crudDao.create(entity2)
      crudDao.create(ExampleEntity(text = "Other entity"))

      val result =
          searchDao.search(
              ExampleSearchQuery(
                  text = "Very specific name for text query test",
                  orderBy = OrderBy.TEXT,
              ),
          )
      assertEquals(result.size, 2)
      assertEquals(result[0].item.text, entity1.text)
      assertEquals(result[1].item.text, entity2.text)
    }
  }

  @Test
  fun orderByOrdersByCorrectData() {
    runBlocking {
      val (initialAgg1, _) = crudDao.create(ExampleEntity(text = "A"))
      val (initialAgg2, _) = crudDao.create(ExampleEntity(text = "B"))

      val result1 =
          searchDao.search(ExampleSearchQuery(orderBy = OrderBy.TEXT, orderDesc = false)).map {
            it.item
          }
      val result2 =
          searchDao.search(ExampleSearchQuery(orderBy = OrderBy.TEXT, orderDesc = true)).map {
            it.item
          }

      result1.indexOf(initialAgg1) shouldBeLessThan result1.indexOf(initialAgg2)
      result2.indexOf(initialAgg1) shouldBeGreaterThan result2.indexOf(initialAgg2)
    }
  }

  @Test
  fun testOrderByCreated() {
    runBlocking {
      val (intialEntity1, _) = crudDao.create(ExampleEntity(text = "A", createdAt = Instant.now()))

      val (initialEntity2, _) =
          crudDao.create(ExampleEntity(text = "B", createdAt = Instant.now().minusSeconds(10000)))

      val result1 = searchDao.search(ExampleSearchQuery(orderDesc = false)).map { it.item }

      val indexOf1 = result1.indexOf(intialEntity1)
      val indexOf2 = result1.indexOf(initialEntity2)
      indexOf1 shouldBeLessThan indexOf2
    }
  }

  @Test
  fun testSearchDaoWithCount() {
    runBlocking {
      val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
      crudDaoWithCount.create(entity1)
      crudDaoWithCount.create(ExampleEntity(text = "Very specific name for text query test 2"))
      crudDaoWithCount.create(ExampleEntity(text = "Other entity"))

      val queryWithLimitLessThanCount = ExampleSearchQuery(limit = 2, offset = 0)
      val result1 = searchDaoWithCount.search(queryWithLimitLessThanCount)
      assertEquals(result1.list.size, queryWithLimitLessThanCount.limit)
      assertEquals(result1.totalCount, 3)

      val queryWithOffsetHigherThanCount = ExampleSearchQuery(limit = 2, offset = 1000)
      val result2 = searchDaoWithCount.search(queryWithOffsetHigherThanCount)
      assertEquals(result2.totalCount, 3)

      val textQuery =
          ExampleSearchQuery(
              text = "Very specific name for text query test",
              limit = 1,
              offset = 0,
              orderBy = OrderBy.TEXT,
          )
      val result3 = searchDaoWithCount.search(textQuery)
      assertEquals(result3.list.size, textQuery.limit)
      assertEquals(result3.totalCount, 2)
      assertEquals(result3.list[0].item.text, entity1.text)
    }
  }

  @Test
  fun verifySnapshot() {
    val agg =
        ExampleEntity(
            id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
            text = "hello world",
        )

    verifyJsonSnapshot(
        "Example.json",
        serializationAdapter.toJson(agg),
        ignoredPaths = listOf("createdAt", "modifiedAt"),
    )
  }
}
