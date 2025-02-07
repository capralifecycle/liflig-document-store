package no.liflig.documentstore.repository

import java.util.UUID
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.testutils.EntityWithIntegerId
import no.liflig.documentstore.testutils.EntityWithStringId
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleId
import no.liflig.documentstore.testutils.ExampleIntegerId
import no.liflig.documentstore.testutils.ExampleStringId
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoWithIntegerId
import no.liflig.documentstore.testutils.exampleRepoWithStringId
import no.liflig.documentstore.utils.currentTimeWithMicrosecondPrecision
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CrudTest {
  @Test
  fun `store and retrieve new entity`() {
    val entity = ExampleEntity(text = "hello world")

    val timeBeforeCreate = currentTimeWithMicrosecondPrecision()
    exampleRepo.create(entity)
    val timeAfterCreate = currentTimeWithMicrosecondPrecision()

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

    val timeBeforeUpdate = currentTimeWithMicrosecondPrecision()
    exampleRepo.update(createdEntity.item.copy(text = "new value"), createdEntity.version)
    val timeAfterUpdate = currentTimeWithMicrosecondPrecision()

    val retrievedEntity = exampleRepo.get(createdEntity.item.id)
    assertNotNull(retrievedEntity)

    assertEquals("new value", retrievedEntity.item.text)
    assertEquals(createdEntity.version.next(), retrievedEntity.version)

    assert(retrievedEntity.modifiedAt.isAfter(timeBeforeUpdate))
    assert(retrievedEntity.modifiedAt.isBefore(timeAfterUpdate))
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
  fun `getOrThrow produces expected exception message`() {
    val exception =
        assertFailsWith<EntityNotFoundException> {
          exampleRepo.getOrThrow(
              ExampleId(UUID.fromString("b0bfc514-eb97-4916-8c32-3cf44bc547ec")),
          )
        }

    assertEquals(
        "Failed to find entity with ID 'b0bfc514-eb97-4916-8c32-3cf44bc547ec' in database (ExampleRepository)",
        exception.message,
    )
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

    val getResult = exampleRepoWithStringId.get(ExampleStringId("test1"))
    assertNotNull(getResult)
    assertEquals(entities[0], getResult.item)

    val listResult =
        exampleRepoWithStringId.listByIds(
            listOf(
                ExampleStringId("test2"),
                ExampleStringId("test3"),
            ),
        )
    assertEquals(2, listResult.size)

    val listEntities = listResult.map { it.item }
    assertContains(listEntities, entities[1])
    assertContains(listEntities, entities[2])

    assertThrows<Exception> {
      exampleRepoWithStringId.create(
          EntityWithStringId(id = ExampleStringId("test1"), text = "test"),
      )
    }
  }

  @Test
  fun `test entity with manual integer ID`() {
    val entities =
        listOf(
            EntityWithIntegerId(id = ExampleIntegerId(1), text = "test"),
            EntityWithIntegerId(id = ExampleIntegerId(2), text = "test"),
            EntityWithIntegerId(id = ExampleIntegerId(3), text = "test"),
        )
    for (entity in entities) {
      exampleRepoWithIntegerId.create(entity)
    }

    val getResult = exampleRepoWithIntegerId.get(ExampleIntegerId(1))
    assertNotNull(getResult)
    assertEquals(entities[0], getResult.item)

    val listResult =
        exampleRepoWithIntegerId.listByIds(
            listOf(
                ExampleIntegerId(2),
                ExampleIntegerId(3),
            ),
        )
    assertEquals(2, listResult.size)

    val listEntities = listResult.map { it.item }
    assertContains(listEntities, entities[1])
    assertContains(listEntities, entities[2])

    assertThrows<Exception> {
      exampleRepoWithIntegerId.create(
          EntityWithIntegerId(id = ExampleIntegerId(1), text = "test"),
      )
    }
  }
}
