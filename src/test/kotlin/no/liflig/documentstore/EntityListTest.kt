package no.liflig.documentstore

import kotlin.test.assertEquals
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import no.liflig.documentstore.entity.filterEntities
import no.liflig.documentstore.entity.forEachEntity
import no.liflig.documentstore.entity.mapEntities
import no.liflig.documentstore.entity.mapEntitiesNotNull
import no.liflig.documentstore.examples.ExampleEntity
import org.junit.jupiter.api.Test

class EntityListTest {
  @Test
  fun `test mapEntities`() {
    val entities =
        listOf(
            VersionedEntity(ExampleEntity(text = "test1"), Version.initial()),
            VersionedEntity(ExampleEntity(text = "test2"), Version.initial()),
        )

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
    val entities =
        listOf(
            VersionedEntity(ExampleEntity(text = "test1"), Version.initial()),
            VersionedEntity(ExampleEntity(text = "test2"), Version.initial()),
        )

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
    val entities =
        listOf(
            VersionedEntity(ExampleEntity(text = "test1"), Version.initial()),
            VersionedEntity(ExampleEntity(text = "test2"), Version.initial()),
        )

    val mappedEntities = entities.filterEntities { entity -> entity.text == "test2" }

    assertEquals(1, mappedEntities.size)
    assertEquals("test2", mappedEntities[0].item.text)
  }

  @Test
  fun `test forEachEntity`() {
    val entities =
        listOf(
            VersionedEntity(ExampleEntity(text = "test1"), Version.initial()),
            VersionedEntity(ExampleEntity(text = "test2"), Version.initial()),
        )

    var combinedText = ""
    entities.forEachEntity { entity -> combinedText += entity.text }

    assertEquals("test1test2", combinedText)
  }
}
