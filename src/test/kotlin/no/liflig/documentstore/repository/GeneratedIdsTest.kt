package no.liflig.documentstore.repository

import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import no.liflig.documentstore.entity.IntegerEntityId
import no.liflig.documentstore.testutils.EntityWithIntegerId
import no.liflig.documentstore.testutils.ExampleIntegerId
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepoWithGeneratedIntegerId
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GeneratedIdsTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
  }

  @Test
  fun `test create`() {
    val entities =
        listOf(
                EntityWithIntegerId(
                    id = ExampleIntegerId(IntegerEntityId.GENERATED),
                    text = "test",
                ),
                EntityWithIntegerId(
                    id = ExampleIntegerId(IntegerEntityId.GENERATED),
                    text = "test",
                ),
                EntityWithIntegerId(
                    id = ExampleIntegerId(IntegerEntityId.GENERATED),
                    text = "test",
                ),
            )
            .map { entity -> exampleRepoWithGeneratedIntegerId.create(entity) }

    // After calling RepositoryJdbi.create, the IDs should now have been set by the database
    entities.forEach { entity -> assertNotEquals(IntegerEntityId.GENERATED, entity.item.id.value) }

    val getResult = exampleRepoWithGeneratedIntegerId.get(entities[0].item.id)
    assertNotNull(getResult)
    assertEquals(entities[0].item, getResult.item)

    val listResult =
        exampleRepoWithGeneratedIntegerId.listByIds(entities.take(2).map { it.item.id })
    assertEquals(2, listResult.size)

    val listEntities = listResult.map { it.item }
    assertContains(listEntities, entities[0].item)
    assertContains(listEntities, entities[1].item)
  }

  @Test
  fun `test batchCreate`() {
    val entitiesToCreate =
        (1..1000).map { number ->
          EntityWithIntegerId(
              id = ExampleIntegerId(IntegerEntityId.GENERATED),
              text = "batch-test-with-generated-id-${number}",
          )
        }
    val createdEntities = exampleRepoWithGeneratedIntegerId.batchCreate(entitiesToCreate)

    assertEquals(entitiesToCreate.size, createdEntities.size)
    // Verify that returned entities are in the same order that we gave them
    for (i in entitiesToCreate.indices) {
      assertEquals(entitiesToCreate[i].text, createdEntities[i].item.text)

      // After calling batchCreate, the IDs should now have been set by the database
      assertNotEquals(IntegerEntityId.GENERATED, createdEntities[i].item.id.value)
    }

    // Verify that fetching out the created entities gives the same results as the ones we got back
    // from batchCreate
    val fetchedEntities =
        exampleRepoWithGeneratedIntegerId.listByIds(createdEntities.map { it.item.id })
    assertEquals(fetchedEntities, createdEntities)
  }
}
