package no.liflig.documentstore

import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.ConflictDaoException
import no.liflig.documentstore.dao.CrudDaoJdbi
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

class ExampleTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

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
  fun verifySnapshot() {
    val agg = ExampleEntity.create(
      id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
      text = "hello world",
      now = Instant.parse("2020-10-11T23:25:00Z")
    )

    verifyJsonSnapshot("Example.json", serializationAdapter.toJson(agg))
  }
}
