package no.liflig.documentstore.repository

import kotlin.test.assertContains
import kotlin.test.assertEquals
import no.liflig.documentstore.testutils.EntityWithStringId
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleStringId
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoForListAll
import no.liflig.documentstore.testutils.exampleRepoWithStringId
import org.junit.jupiter.api.Test

class ListTest {
  @Test
  fun `test listAll`() {
    val createdEntities = (0..9).map { number -> ExampleEntity(text = "list-all-test-${number}") }
    for (entity in createdEntities) {
      // Use create instead of batchCreate here, since we want to verify that they are sorted by
      // createdAt
      exampleRepoForListAll.create(entity)
    }

    val all = exampleRepoForListAll.listAll()
    assertEquals(createdEntities.size, all.size)
    assertEquals(createdEntities, all.map { it.item })
  }

  @Test
  fun `test listByIds for UuidEntityId`() {
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

  /**
   * We want to test that [listByIds][no.liflig.documentstore.repository.RepositoryJdbi.listByIds]
   * works with both [UuidEntityId][no.liflig.documentstore.entity.UuidEntityId] and
   * [StringEntityId][no.liflig.documentstore.entity.StringEntityId], since we want to verify that
   * both ID types work with the `registerArrayType` we use in
   * [no.liflig.documentstore.DocumentStorePlugin].
   */
  @Test
  fun `test listByIds for StringEntityId`() {
    val (entity1, _) =
        exampleRepoWithStringId.create(
            EntityWithStringId(
                id = ExampleStringId("entity-1"),
                text = "Test 1",
            ),
        )
    val (entity2, _) =
        exampleRepoWithStringId.create(
            EntityWithStringId(
                id = ExampleStringId("entity-2"),
                text = "Test 2",
            ),
        )
    // Third unused entity to verify that we only fetch entity 1 and 2
    exampleRepoWithStringId.create(
        EntityWithStringId(
            id = ExampleStringId("entity-3"),
            text = "Test 3",
        ),
    )

    val results = exampleRepoWithStringId.listByIds(listOf(entity1.id, entity2.id))
    assertEquals(results.size, 2)

    val texts = results.map { it.item.text }
    assertContains(texts, entity1.text)
    assertContains(texts, entity2.text)
  }
}
