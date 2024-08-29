package no.liflig.documentstore

import java.time.Instant
import java.util.*
import kotlin.concurrent.thread
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.serialization.encodeToString
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.examples.EntityWithStringId
import no.liflig.documentstore.examples.ExampleEntity
import no.liflig.documentstore.examples.ExampleId
import no.liflig.documentstore.examples.ExampleRepository
import no.liflig.documentstore.examples.ExampleRepositoryWithCount
import no.liflig.documentstore.examples.ExampleRepositoryWithStringEntityId
import no.liflig.documentstore.examples.ExampleStringId
import no.liflig.documentstore.examples.OrderBy
import no.liflig.documentstore.examples.UniqueFieldAlreadyExists
import no.liflig.documentstore.examples.json
import no.liflig.documentstore.repository.ConflictRepositoryException
import no.liflig.documentstore.repository.transactional
import no.liflig.snapshot.verifyJsonSnapshot
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RepositoryTest {
  private val jdbi = createTestDatabase()
  private val exampleRepo = ExampleRepository(jdbi)
  private val exampleRepoWithCount = ExampleRepositoryWithCount(jdbi)
  private val exampleRepoWithStringId = ExampleRepositoryWithStringEntityId(jdbi)

  @Test
  fun `store and retrieve new entity`() {
    val entity = ExampleEntity(text = "hello world")

    val timeBeforeCreate = Instant.now()
    exampleRepo.create(entity)
    val timeAfterCreate = Instant.now()

    val retrievedEntity = exampleRepo.get(entity.id)

    assertNotNull(retrievedEntity)
    assertEquals(Version.initial(), retrievedEntity.version)
    assertEquals(entity, retrievedEntity.item)

    for (timestamp in sequenceOf(retrievedEntity.createdAt, retrievedEntity.modifiedAt)) {
      assert(timestamp.isAfter(timeBeforeCreate))
      assert(timestamp.isBefore(timeAfterCreate))
    }
  }

  @Test
  fun `update with wrong version`() {
    val (entity, version) = exampleRepo.create(ExampleEntity(text = "hello world"))

    assertFailsWith<ConflictRepositoryException> { exampleRepo.update(entity, version.next()) }
  }

  @Test
  fun `delete entity`() {
    val entity = ExampleEntity(text = "hello world")

    assertFailsWith<ConflictRepositoryException> {
      exampleRepo.delete(entity.id, Version.initial())
    }

    val createdEntity = exampleRepo.create(entity)
    assertEquals(Version.initial(), createdEntity.version)

    exampleRepo.delete(entity.id, Version.initial())

    val entityAfterDelete = exampleRepo.get(entity.id)
    assertNull(entityAfterDelete)
  }

  @Test
  fun `update entity`() {
    val createdEntity = exampleRepo.create(ExampleEntity(text = "hello world"))

    exampleRepo.update(createdEntity.item.copy(text = "new value"), createdEntity.version)

    val retrievedEntity = exampleRepo.get(createdEntity.item.id)
    assertNotNull(retrievedEntity)

    assertEquals("new value", retrievedEntity.item.text)
    assertNotEquals(createdEntity.version, retrievedEntity.version)
  }

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
  fun `getByPredicate returns expected matches`() {
    val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
    exampleRepo.create(entity1)
    val entity2 = ExampleEntity(text = "Very specific name for text query test 2")
    exampleRepo.create(entity2)
    exampleRepo.create(ExampleEntity(text = "Other entity"))

    val result =
        // Uses RepositoryJdbi.getByPredicate
        exampleRepo.search(
            text = "Very specific name for text query test",
            orderBy = OrderBy.TEXT,
        )
    assertEquals(result.size, 2)
    assertEquals(result[0].item.text, entity1.text)
    assertEquals(result[1].item.text, entity2.text)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `orderBy orders by correct data`(orderDesc: Boolean) {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "A"))
    val (entity2, _) = exampleRepo.create(ExampleEntity(text = "B"))

    val result = exampleRepo.search(orderBy = OrderBy.TEXT, orderDesc = orderDesc).map { it.item }

    val indexOf1 = result.indexOf(entity1)
    assertNotEquals(-1, indexOf1)

    val indexOf2 = result.indexOf(entity2)
    assertNotEquals(-1, indexOf2)

    if (orderDesc) {
      assert(indexOf1 > indexOf2)
    } else {
      assert(indexOf1 < indexOf2)
    }
  }

  @Test
  fun `getByPredicate should order by created_at by default`() {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "A"))
    val (entity2, _) = exampleRepo.create(ExampleEntity(text = "B"))

    val result = exampleRepo.search(orderDesc = false).map { it.item }

    val indexOf1 = result.indexOf(entity1)
    assertNotEquals(-1, indexOf1)

    val indexOf2 = result.indexOf(entity2)
    assertNotEquals(-1, indexOf2)

    assert(indexOf1 < indexOf2)
  }

  @Test
  fun `test listByIds`() {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "Test 1"))
    val (entity2, _) = exampleRepo.create(ExampleEntity(text = "Test 2"))
    // Third unused entity to verify that we only fetch entity 1 and 2
    exampleRepo.create(ExampleEntity(text = "Test 3"))

    val results = exampleRepo.listByIds(listOf(entity1.id, entity2.id))
    assertEquals(results.size, 2)

    val texts = results.map { it.item.text }
    assertContains(texts, entity1.text)
    assertContains(texts, entity2.text)
  }

  @Test
  fun `test getByPredicateWithTotalCount`() {
    val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
    exampleRepoWithCount.create(entity1)
    exampleRepoWithCount.create(ExampleEntity(text = "Very specific name for text query test 2"))
    exampleRepoWithCount.create(ExampleEntity(text = "Other entity"))

    val limitLessThanCount = 2
    val result1 = exampleRepoWithCount.search(limit = limitLessThanCount)
    assertEquals(result1.list.size, limitLessThanCount)
    assertEquals(result1.totalCount, 3)

    val offsetHigherThanCount = 1000
    val result2 = exampleRepoWithCount.search(offset = offsetHigherThanCount)
    assertEquals(result2.totalCount, 3)

    val result3 =
        exampleRepoWithCount.search(
            text = "Very specific name for text query test",
            limit = 1,
            offset = 0,
            orderBy = OrderBy.TEXT,
        )
    assertEquals(result3.list.size, 1)
    assertEquals(result3.totalCount, 2)
    assertEquals(result3.list[0].item.text, entity1.text)
  }

  @Test
  fun `test entity with string ID`() {
    val entities =
        listOf(
            EntityWithStringId(id = ExampleStringId("test1"), text = "test"),
            EntityWithStringId(id = ExampleStringId("test2"), text = "test"),
            EntityWithStringId(id = ExampleStringId("test3"), text = "test"),
        )
    for (entity in entities) {
      exampleRepoWithStringId.create(entity)
    }

    val result1 = exampleRepoWithStringId.get(ExampleStringId("test1"))
    assertNotNull(result1)
    assertEquals(entities[0], result1.item)

    val result2 =
        exampleRepoWithStringId.listByIds(
            listOf(
                ExampleStringId("test2"),
                ExampleStringId("test3"),
            ),
        )
    assertEquals(2, result2.size)

    val resultEntities = result2.map { it.item }
    assertContains(resultEntities, entities[1])
    assertContains(resultEntities, entities[2])

    assertThrows<Exception> {
      exampleRepoWithStringId.create(
          EntityWithStringId(
              id = ExampleStringId("test1"),
              text = "test",
          ),
      )
    }
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in create`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    assertFailsWith<UniqueFieldAlreadyExists> {
      exampleRepo.create(ExampleEntity(text = "A", uniqueField = 1))
      exampleRepo.create(ExampleEntity(text = "B", uniqueField = 1))
    }
  }

  @Test
  fun `mapCreateOrUpdateException catches and maps exceptions in update`() {
    /** We map to this custom exception in [ExampleRepository.mapCreateOrUpdateException]. */
    assertFailsWith<UniqueFieldAlreadyExists> {
      val (entity1, version1) = exampleRepo.create(ExampleEntity(text = "A", uniqueField = 2))
      exampleRepo.create(ExampleEntity(text = "B", uniqueField = 3))

      exampleRepo.update(entity1.copy(uniqueField = 3), version1)
    }
  }

  @Test
  fun verifySnapshot() {
    val entity =
        ExampleEntity(
            id = ExampleId(UUID.fromString("928f6ef3-6873-454a-a68d-ef3f5d7963b5")),
            text = "hello world",
        )

    verifyJsonSnapshot(
        "ExampleEntity.json",
        json.encodeToString(entity),
    )
  }
}
