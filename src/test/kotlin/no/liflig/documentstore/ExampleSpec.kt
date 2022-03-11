package no.liflig.documentstore

import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.repository.ConflictRepositoryException
import no.liflig.snapshot.verifyJsonSnapshot
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

object ExampleSpec : Spek({
  describe("ExampleSpec") {
    val jdbi = createTestDatabase()
    val repository = ExampleRepository(jdbi)

    it("can store and retrieve a new aggregate") {
      runBlocking {
        val agg = ExampleAggregate.create("hello world")

        repository
          .create(agg)

        val read = repository
          .get(agg.id)

        assertNotNull(read)
        assertEquals(Version.initial(), read.version)
        assertEquals(agg, read.item)
      }
    }

    it("fails if wrong version when updating") {
      runBlocking {
        val agg = ExampleAggregate.create("hello world")

        val storeResult = repository
          .create(agg)

        assertFailsWith<ConflictRepositoryException> {
          repository
            .update(agg, storeResult.version.next())
        }
      }
    }

    it("can delete an aggregate") {
      runBlocking {
        val agg = ExampleAggregate.create("hello world")

        assertFailsWith<ConflictRepositoryException> {
          repository.delete(agg.id, Version.initial())
        }

        val res2 = repository.create(agg)
        assertEquals(Version.initial(), res2.version)

        val res3 = repository.delete(agg.id, Version.initial())
        assertEquals(Unit, res3)

        val res4 = repository.get(agg.id)
        assertNull(res4)
      }
    }

    it("can update an aggregate") {
      runBlocking {
        val (initialAgg, initialVersion) = repository
          .create(ExampleAggregate.create("hello world"))

        val updatedAgg = initialAgg.updateText("new value")
        repository
          .update(updatedAgg, initialVersion)

        val res = repository
          .get(updatedAgg.id)

        assertNotNull(res)
        val (agg, version) = res

        assertEquals("new value", agg.text)
        assertNotEquals(initialVersion, version)
      }
    }

    it("has same snapshot as previously") {
      val agg = ExampleAggregate.create(
        id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
        text = "hello world",
        now = Instant.parse("2020-10-11T23:25:00Z")
      )

      verifyJsonSnapshot("Example.json", repository.toJson(agg))
    }
  }
})
