package no.liflig.documentstore.repository

import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.OrderBy
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoWithCount
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class GetByPredicateTest {
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
  fun `test getByPredicateWithTotalCount`() {
    val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
    exampleRepoWithCount.create(entity1)
    exampleRepoWithCount.create(ExampleEntity(text = "Very specific name for text query test 2"))
    exampleRepoWithCount.create(ExampleEntity(text = "Other entity"))

    val limitLessThanCount = 2
    val result1 = exampleRepoWithCount.searchWithTotalCount(limit = limitLessThanCount)
    assertEquals(result1.list.size, limitLessThanCount)
    assertEquals(result1.totalCount, 3)

    val offsetHigherThanCount = 1000
    val result2 = exampleRepoWithCount.searchWithTotalCount(offset = offsetHigherThanCount)
    assertEquals(result2.totalCount, 3)

    val result3 =
        exampleRepoWithCount.searchWithTotalCount(
            text = "Very specific name for text query test",
            limit = 1,
            offset = 0,
            orderBy = OrderBy.TEXT,
        )
    assertEquals(result3.list.size, 1)
    assertEquals(result3.totalCount, 2)
    assertEquals(result3.list[0].item.text, entity1.text)
  }
}
