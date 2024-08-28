package no.liflig.documentstore

import java.time.Instant
import kotlin.test.assertEquals
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.filterEntities
import no.liflig.documentstore.entity.forEachEntity
import no.liflig.documentstore.entity.mapEntities
import no.liflig.documentstore.entity.mapEntitiesNotNull
import no.liflig.documentstore.examples.ExampleEntity
import no.liflig.documentstore.repository.ListWithTotalCount
import org.junit.jupiter.api.Test

class VersionedListUtilsTest {
  @Test
  fun `test mapEntities`() {
    val entities = createEntities("test1", "test2")

    val mappedEntities = entities.mapEntities { entity -> entity.copy(moreText = "New text!") }
    assertEquals(entities.size, mappedEntities.size)

    for (i in entities.indices) {
      val original = entities[i]
      val mapped = mappedEntities[i]

      assertEquals(original.version, mapped.version)
      assertEquals(original.item.text, mapped.item.text)
      assertEquals("New text!", mapped.item.moreText)
    }
  }

  @Test
  fun `test mapEntitiesNotNull`() {
    val entities = createEntities("test1", "test2")

    val mappedEntities =
        entities.mapEntitiesNotNull { entity ->
          if (entity.text == "test1") {
            null
          } else {
            entity
          }
        }

    assertEquals(1, mappedEntities.size)
    assertEquals("test2", mappedEntities[0].item.text)
  }

  @Test
  fun `test filterEntities`() {
    val entities = createEntities("test1", "test2")

    val mappedEntities = entities.filterEntities { entity -> entity.text == "test2" }

    assertEquals(1, mappedEntities.size)
    assertEquals("test2", mappedEntities[0].item.text)
  }

  @Test
  fun `test forEachEntity`() {
    val entities = createEntities("test1", "test2")

    var combinedText = ""
    entities.forEachEntity { entity -> combinedText += entity.text }

    assertEquals("test1test2", combinedText)
  }

  @Test
  fun `test mapEntities for ListWithTotalCount`() {
    val entities = createEntitiesWithTotalCount("test1", "test2")

    val mappedEntities = entities.mapEntities { entity -> entity.copy(moreText = "New text!") }
    assertEquals(entities.list.size, mappedEntities.list.size)
    assertEquals(entities.totalCount, mappedEntities.totalCount)

    for (i in entities.list.indices) {
      val original = entities.list[i]
      val mapped = mappedEntities.list[i]

      assertEquals(original.version, mapped.version)
      assertEquals(original.item.text, mapped.item.text)
      assertEquals("New text!", mapped.item.moreText)
    }
  }

  @Test
  fun `test mapEntitiesNotNull for ListWithTotalCount`() {
    val entities = createEntitiesWithTotalCount("test1", "test2")

    val mappedEntities =
        entities.mapEntitiesNotNull { entity ->
          if (entity.text == "test1") {
            null
          } else {
            entity
          }
        }

    assertEquals(1, mappedEntities.list.size)
    assertEquals(entities.totalCount, mappedEntities.totalCount)
    assertEquals("test2", mappedEntities.list[0].item.text)
  }

  @Test
  fun `test filterEntities for ListWithTotalCount`() {
    val entities = createEntitiesWithTotalCount("test1", "test2")

    val mappedEntities = entities.filterEntities { entity -> entity.text == "test2" }

    assertEquals(1, mappedEntities.list.size)
    assertEquals(entities.totalCount, mappedEntities.totalCount)
    assertEquals("test2", mappedEntities.list[0].item.text)
  }

  @Test
  fun `test forEachEntity for ListWithTotalCount`() {
    val entities = createEntitiesWithTotalCount("test1", "test2")

    var combinedText = ""
    entities.forEachEntity { entity -> combinedText += entity.text }

    assertEquals("test1test2", combinedText)
  }
}

private fun createEntities(vararg texts: String): List<Versioned<ExampleEntity>> {
  return texts.map { text ->
    Versioned(
        ExampleEntity(text = text),
        Version.initial(),
        createdAt = Instant.now(),
        modifiedAt = Instant.now(),
    )
  }
}

private fun createEntitiesWithTotalCount(
    vararg texts: String
): ListWithTotalCount<Versioned<ExampleEntity>> {
  return ListWithTotalCount(
      list = createEntities(*texts),
      totalCount = 100,
  )
}
