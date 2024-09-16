package no.liflig.documentstore.repository

import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import no.liflig.documentstore.entity.mapEntities
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.examples.ExampleEntity
import no.liflig.documentstore.testutils.jdbi
import org.junit.jupiter.api.Test

class TransactionTest {
  @Test
  fun `complete transaction succeeds`() {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    transactional(jdbi) {
      exampleRepo.update(entity1.copy(text = "Two"), version1)
      exampleRepo.update(entity2.copy(text = "Two"), version2)
      exampleRepo.get(entity2.id)
    }

    assertNotEquals(entity1.id, entity2.id)
    assertEquals("Two", exampleRepo.get(entity1.id)!!.item.text)
    assertEquals("Two", exampleRepo.get(entity1.id)!!.item.text)
  }

  @Test
  fun `failed transaction rolls back`() {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    assertFailsWith<ConflictRepositoryException> {
      transactional(jdbi) {
        exampleRepo.update(entity1.copy(text = "Two"), version1)
        exampleRepo.update(entity2.copy(text = "Two"), version2.next())
      }
    }

    assertEquals("One", exampleRepo.get(entity1.id)!!.item.text)
    assertEquals("One", exampleRepo.get(entity2.id)!!.item.text)
  }

  @Test
  fun `transaction within transaction rolls back as expected`() {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))
    val (entity2, version2) = exampleRepo.create(ExampleEntity(text = "One"))

    try {
      transactional(jdbi) {
        exampleRepo.update(entity1.copy(text = "Two"), version1)
        transactional(jdbi) { exampleRepo.update(entity2.copy(text = "Two"), version2) }
        throw ConflictRepositoryException("test")
      }
    } catch (_: ConflictRepositoryException) {}

    assertEquals("One", exampleRepo.get(entity1.id)!!.item.text)
    assertEquals("One", exampleRepo.get(entity2.id)!!.item.text)
  }

  @Test
  fun `transaction prevents race conditions`() {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "Text"))

    // Without locking, we should be getting ConflictRepositoryException when concurrent processes
    // attempt to update the same row. With locking, each transaction will wait until lock is
    // released before reading.
    val threads =
        (1 until 100)
            .map { index ->
              thread {
                transactional(jdbi) {
                  val first = exampleRepo.get(entity1.id, true)!!
                  val updated = first.item.copy(text = index.toString())
                  exampleRepo.update(updated, first.version)
                }
              }
            }
            .toList()

    // We want to explicitly do this after the .map, so the threads actually run simultaneously
    for (thread in threads) {
      thread.join()
    }

    assertEquals(100, exampleRepo.get(entity1.id)!!.version.value)
  }

  @Test
  fun `get returns updated data within transaction`() {
    val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "One"))

    val result =
        transactional(jdbi) {
          exampleRepo.update(entity1.copy(text = "Two"), version1)
          exampleRepo.get(entity1.id)?.item?.text
        }

    assertEquals("Two", result)
  }

  @Test
  fun `batchCreate rolls back on failed transaction`() {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "batchCreate transaction test") }

    assertFailsWith<Exception> {
      transactional(jdbi) {
        exampleRepo.batchCreate(entitiesToCreate)
        throw Exception("Rolling back transaction")
      }
    }

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    assertEquals(0, createdEntities.size)
  }

  @Test
  fun `batchUpdate rolls back on failed transaction`() {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "Original") }
    exampleRepo.batchCreate(entitiesToCreate)

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })
    val updatedEntities = createdEntities.mapEntities { it.copy(text = "Updated") }

    assertFailsWith<Exception> {
      transactional(jdbi) {
        exampleRepo.batchUpdate(updatedEntities)
        throw Exception("Rolling back transaction")
      }
    }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.item.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
    for (entity in fetchedEntities) {
      assertEquals("Original", entity.item.text)
    }
  }

  @Test
  fun `batchDelete rolls back on failed transaction`() {
    val entitiesToCreate = (1..10).map { ExampleEntity(text = "Original") }
    exampleRepo.batchCreate(entitiesToCreate)

    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })

    assertFailsWith<Exception> {
      transactional(jdbi) {
        exampleRepo.batchDelete(createdEntities)
        throw Exception("Rolling back transaction")
      }
    }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.item.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
  }
}
