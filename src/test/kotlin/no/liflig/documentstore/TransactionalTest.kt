package no.liflig.documentstore

import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeLessThan
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.ConflictDaoException
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.transactional
import no.liflig.documentstore.entity.Version
import no.liflig.snapshot.verifyJsonSnapshot
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TransactionalTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)
  val searchRepository = ExampleSearchRepository(jdbi, "example", serializationAdapter)

  @Test
  fun storeAndRetrieveNewEntity() {
    runBlocking {
      val agg = ExampleEntity.create("hello world")

      dao
        .create(agg)

      val read = dao
        .get(agg.id)

      assertNotNull(read)
      assertEquals(Version.initial(), read.version)
      assertEquals(agg, read.item)
    }
  }

  @Test
  fun updateWithWrongVersion() {
    runBlocking {
      val agg = ExampleEntity.create("hello world")

      val storeResult = dao
        .create(agg)

      assertFailsWith<ConflictDaoException> {
        dao
          .update(agg, storeResult.version.next())
      }
    }
  }

  @Test
  fun deleteEntity() {
    runBlocking {
      val agg = ExampleEntity.create("hello world")

      assertFailsWith<ConflictDaoException> {
        dao.delete(agg.id, Version.initial())
      }

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
      val (initialAgg, initialVersion) = dao
        .create(ExampleEntity.create("hello world"))

      val updatedAgg = initialAgg.updateText("new value")
      dao
        .update(updatedAgg, initialVersion)

      val res = dao
        .get(updatedAgg.id)

      assertNotNull(res)
      val (agg, version) = res

      assertEquals("new value", agg.text)
      assertNotEquals(initialVersion, version)
    }
  }

  @Test
  fun completeTransactionSucceeds() {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao
        .create(ExampleEntity.create("One"))

      transactional(jdbi) {
        dao.update(initialAgg1.updateText("Two"), initialVersion1)
        dao.update(initialAgg2.updateText("Two"), initialVersion2)
        dao.get(initialAgg2.id)
      }

      assertNotEquals(initialAgg1.id, initialAgg2.id)
      assertEquals("Two", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("Two", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @Test
  fun failedTransactionRollsBack() {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao
        .create(ExampleEntity.create("One"))

      try {
        transactional(jdbi) {
          dao.update(initialAgg1.updateText("Two"), initialVersion1)
          dao.update(initialAgg2.updateText("Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {
      }

      assertEquals("One", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @Test
  fun failedTransactionWithExplicitHandleStartedOutsideRollsBack() {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao
        .create(ExampleEntity.create("One"))

      var exceptionThrown = false
      try {
        jdbi.open().useTransaction<Exception> { handle ->
          runBlocking {
            dao.update(initialAgg1.updateText("Two"), initialVersion1, handle)
            dao.update(initialAgg2.updateText("Two"), initialVersion2.next(), handle)
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

  @Test
  fun failedTransactionFactoryRollsBack() {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create("One"))
      val (initialAgg2, initialVersion2) = dao
        .create(ExampleEntity.create("One"))

      try {
        transactional(jdbi) {
          dao.update(initialAgg1.updateText("Two"), initialVersion1)
          dao.update(initialAgg2.updateText("Two"), initialVersion2.next())
        }
      } catch (_: ConflictDaoException) {
      }

      assertEquals("One", dao.get(initialAgg1.id)!!.item.text)
      assertEquals("One", dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @Test
  fun transactionWithinTransactionRollsBackAsExpected() {
    runBlocking {
      val initialValue = "Initial"
      val updatedVaue = "Updated value"
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create(initialValue))
      val (initialAgg2, initialVersion2) = dao
        .create(ExampleEntity.create(initialValue))

      try {
        transactional(jdbi) {
          dao.update(initialAgg1.updateText(updatedVaue), initialVersion1)
          transactional(jdbi) {
            dao.update(initialAgg2.updateText(updatedVaue), initialVersion2)
          }
          throw ConflictDaoException()
        }
      } catch (_: ConflictDaoException) {
      }

      assertEquals(initialValue, dao.get(initialAgg1.id)!!.item.text)
      assertEquals(initialValue, dao.get(initialAgg2.id)!!.item.text)
    }
  }

  @Test
  fun getReturnsUpdatedDataWithinTransaction() {
    runBlocking {
      val (initialAgg1, initialVersion1) = dao
        .create(ExampleEntity.create("One"))

      val result = transactional(jdbi) {
        dao.update(initialAgg1.updateText("Two"), initialVersion1)
        dao.get(initialAgg1.id)
      }

      assertEquals("Two", result?.item?.text)
    }
  }

  @Test
  fun orderByOrdersByCorrectData() {
    runBlocking {
      val (initialAgg1, _) = dao
        .create(ExampleEntity.create("A"))
      val (initialAgg2, _) = dao
        .create(ExampleEntity.create("B"))

      val result1 = searchRepository.search(orderBy = ExampleSearchRepository.OrderBy.TEXT, orderDesc = false)
        .map { it.item }
      val result2 = searchRepository.search(orderBy = ExampleSearchRepository.OrderBy.TEXT, orderDesc = true)
        .map { it.item }

      result1.indexOf(initialAgg1) shouldBeLessThan result1.indexOf(initialAgg2)
      result2.indexOf(initialAgg1) shouldBeGreaterThan result2.indexOf(initialAgg2)
    }
  }

  @Test
  fun test() {
    runBlocking {
      val (initialAgg1, _) = dao
        .create(ExampleEntity.create("A", now = Instant.now()))

      val (initialAgg2, _) = dao
        .create(ExampleEntity.create("B", now = Instant.now().minusSeconds(10000)))

      val result1 = searchRepository.search(orderDesc = false)
        .map { it.item }

      val indexOf1 = result1.indexOf(initialAgg1)
      val indexOf2 = result1.indexOf(initialAgg2)
      println(indexOf1)
      println(indexOf2)
      indexOf1 shouldBeLessThan indexOf2

      Instant.now().minusMillis(10000) shouldBeLessThan Instant.now()
    }
  }

  fun verifySnapshot() {
    val agg = ExampleEntity.create(
      id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
      text = "hello world",
      now = Instant.parse("2020-10-11T23:25:00Z")
    )

    verifyJsonSnapshot("Example.json", serializationAdapter.toJson(agg))
  }
}
