package no.liflig.documentstore.repository

import java.time.Instant
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.testutils.EntityWithStringId
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleStringId
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoWithStringId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class CrudTest {
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
  fun `update entity`() {
    val createdEntity = exampleRepo.create(ExampleEntity(text = "hello world"))

    exampleRepo.update(createdEntity.item.copy(text = "new value"), createdEntity.version)

    val retrievedEntity = exampleRepo.get(createdEntity.item.id)
    assertNotNull(retrievedEntity)

    assertEquals("new value", retrievedEntity.item.text)
    assertNotEquals(createdEntity.version, retrievedEntity.version)
  }

  @Test
  fun `update with wrong version`() {
    val (entity, version) = exampleRepo.create(ExampleEntity(text = "hello world"))

    assertFailsWith<ConflictRepositoryException> {
      exampleRepo.update(
          entity,
          version.next(),
      )
    }
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
}
