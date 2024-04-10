package no.liflig.documentstore

import io.kotest.matchers.comparables.shouldBeLessThan
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
import no.liflig.documentstore.examples.ExampleSearchQuery
import no.liflig.documentstore.examples.ExampleSearchRepository
import no.liflig.documentstore.examples.ExampleSearchRepositoryWithCount
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
class TransactionalTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)
  val searchRepository = ExampleSearchRepository(jdbi, "example", serializationAdapter)

  // Separate DAOs to avoid other tests interfering with the count returned by
  // SearchRepositoryWithCount
  val daoWithCount = CrudDaoJdbi(jdbi, "example_with_count", serializationAdapter)
  val searchRepositoryWithCount =
      ExampleSearchRepositoryWithCount(jdbi, "example_with_count", serializationAdapter)

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
      val agg = ExampleEntity.create("hello world")

      dao.create(agg)

      val read = dao.get(agg.id)

      assertNotNull(read)
      assertEquals(Version.initial(), read.version)
      assertEquals(agg, read.item)
    }
  }

  @Test
  fun updateWithWrongVersion() {
    runBlocking {
      val agg = ExampleEntity.create("hello world")

      val storeResult = dao.create(agg)

      assertFailsWith<ConflictDaoException> { dao.update(agg, storeResult.version.next()) }
    }
  }

  @Test
  fun deleteEntity() {
    runBlocking {
      val agg = ExampleEntity.create("hello world")

      assertFailsWith<ConflictDaoException> { dao.delete(agg.id, Version.initial()) }

      val res2 = dao.create(agg)
      assertEquals(Version.initial(), res2.version)

      val res3 = dao.delete(agg.id, Version.initial())
      assertEquals(Unit, res3)

      val res4 = dao.get(agg.id)
      assertNull(res4)
    }
  }

  @Test
  fun updateEntity() {
    runBlocking {
      val (initialAgg, initialVersion) = dao.create(ExampleEntity.create("hello world"))

      val updatedAgg = initialAgg.updateText("new value")
      dao.update(updatedAgg, initialVersion)

      val res = dao.get(updatedAgg.id)

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
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao.create(ExampleEntity.create("One"))

      transactionBlock(jdbi) {
        dao.update(initialAgg1.updateText("Two"), initialVersion1)
        dao.update(initialAgg2.updateText("Two"), initialVersion2)
        dao.get(initialAgg2.id)
      }

      assertNotEquals(initialAgg1.id, initialAgg2.id)
      assertEquals("Two", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("Two", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest()
  @MethodSource("getTransactionFunctions")
  fun failedTransactionRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao.create(ExampleEntity.create("One"))

      try {
        transactionBlock(jdbi) {
          dao.update(initialAgg1.updateText("Two"), initialVersion1)
          dao.update(initialAgg2.updateText("Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {}

      assertEquals("One", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun failedTransactionWithExplicitHandleStartedOutsideRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao.create(ExampleEntity.create("One"))

      var exceptionThrown = false
      try {
        transactionBlock(jdbi) {
          runBlocking {
            dao.update(initialAgg1.updateText("Two"), initialVersion1)
            dao.update(initialAgg2.updateText("Two"), initialVersion2.next())
          }
        }
      } catch (_: ConflictDaoException) {
        exceptionThrown = true
      }

      assert(exceptionThrown)
      assertEquals("One", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun failedTransactionFactoryRollsBack(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao.create(ExampleEntity.create("One"))

      try {
        transactionBlock(jdbi) {
          dao.update(initialAgg1.updateText("Two"), initialVersion1)
          dao.update(initialAgg2.updateText("Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {}

      assertEquals("One", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun transactionWithinTransactionRollsBackAsExpected(transactionBlock: Transactional) {
    runBlocking {
      val initialValue = "Initial"
      val updatedVaue = "Updated value"
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create(initialValue))
      val (initialAgg2, initialVersion2) = dao.create(ExampleEntity.create(initialValue))

      try {
        transactionBlock(jdbi) {
          dao.update(initialAgg1.updateText(updatedVaue), initialVersion1)
          transactionBlock(jdbi) {
            dao.update(initialAgg2.updateText(updatedVaue), initialVersion2)
          }
          throw ConflictDaoException()
        }
      } catch (_: ConflictDaoException) {}

      assertEquals(initialValue, dao.get(initialAgg1.id)!!.item.text)
      assertEquals(initialValue, dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun transactionWithForUpdateLocksRows(transactionBlock: Transactional) {
    runBlocking {
      val initialValue = "Initial"
      val (initialAgg1, _) = dao.create(ExampleEntity.create(initialValue))

      // Without locking, we should be getting ConflictDaoException when concurrent processes
      // attempt to update the same row. With locking, each transaction will wait until lock
      // is released before reading
      (1 until 100)
          .map {
            async(Dispatchers.IO) {
              transactionBlock(jdbi) {
                val first = dao.get(initialAgg1.id, true)!!
                val updated = first.item.updateText(it.toString())
                dao.update(updated, first.version)
              }
            }
          }
          .awaitAll()

      assertEquals(100, dao.get(initialAgg1.id)!!.version.value)
    }
  }

  @ParameterizedTest
  @MethodSource("getTransactionFunctions")
  fun getReturnsUpdatedDataWithinTransaction(transactionBlock: Transactional) {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao.create(ExampleEntity.create("One"))

      val result =
          transactionBlock(jdbi) {
            dao.update(initialAgg1.updateText("Two"), initialVersion1)
            dao.get(initialAgg1.id)?.item?.text
          }

      assertEquals("Two", result)
    }
  }

  @Test
  fun searchRepositoryTextQuery() {
    runBlocking {
      val entity1 = ExampleEntity.create("Very specific name for text query test 1")
      dao.create(entity1)
      val entity2 = ExampleEntity.create("Very specific name for text query test 2")
      dao.create(entity2)
      dao.create(ExampleEntity.create("Other entity"))

      val result =
          searchRepository.search(
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
      val (initialAgg1, _) = dao.create(ExampleEntity.create("A"))
      val (initialAgg2, _) = dao.create(ExampleEntity.create("B"))

      val result1 =
          searchRepository
              .search(ExampleSearchQuery(orderBy = OrderBy.TEXT, orderDesc = false))
              .map { it.item }
      val result2 =
          searchRepository
              .search(ExampleSearchQuery(orderBy = OrderBy.TEXT, orderDesc = true))
              .map { it.item }

      result1.indexOf(initialAgg1) shouldBeLessThan result1.indexOf(initialAgg2)
      result2.indexOf(initialAgg1) shouldBeGreaterThan result2.indexOf(initialAgg2)
    }
  }

  @Test
  fun test() {
    runBlocking {
      val (initialAgg1, _) = dao.create(ExampleEntity.create("A", now = Instant.now()))

      val (initialAgg2, _) =
          dao.create(ExampleEntity.create("B", now = Instant.now().minusSeconds(10000)))

      val result1 = searchRepository.search(ExampleSearchQuery(orderDesc = false)).map { it.item }

      val indexOf1 = result1.indexOf(initialAgg1)
      val indexOf2 = result1.indexOf(initialAgg2)
      println(indexOf1)
      println(indexOf2)
      indexOf1 shouldBeLessThan indexOf2

      Instant.now().minusMillis(10000) shouldBeLessThan Instant.now()
    }
  }

  @Test
  fun testSearchRepositoryWithCount() {
    runBlocking {
      val entity1 = ExampleEntity.create("Very specific name for text query test 1")
      daoWithCount.create(entity1)
      daoWithCount.create(ExampleEntity.create("Very specific name for text query test 2"))
      daoWithCount.create(ExampleEntity.create("Other entity"))

      val queryWithLimitLessThanCount = ExampleSearchQuery(limit = 2, offset = 0)
      val result1 = searchRepositoryWithCount.search(queryWithLimitLessThanCount)
      assertEquals(result1.list.size, queryWithLimitLessThanCount.limit)
      assertEquals(result1.totalCount, 3)

      val queryWithOffsetHigherThanCount = ExampleSearchQuery(limit = 2, offset = 1000)
      val result2 = searchRepositoryWithCount.search(queryWithOffsetHigherThanCount)
      assertEquals(result2.totalCount, 3)

      val textQuery =
          ExampleSearchQuery(
              text = "Very specific name for text query test",
              limit = 1,
              offset = 0,
              orderBy = OrderBy.TEXT,
          )
      val result3 = searchRepositoryWithCount.search(textQuery)
      assertEquals(result3.list.size, textQuery.limit)
      assertEquals(result3.totalCount, 2)
      assertEquals(result3.list[0].item.text, entity1.text)
    }
  }

  @Test
  fun verifySnapshot() {
    val agg =
        ExampleEntity.create(
            id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
            text = "hello world",
            now = Instant.parse("2020-10-11T23:25:00Z"),
        )

    verifyJsonSnapshot("Example.json", serializationAdapter.toJson(agg))
  }
}
