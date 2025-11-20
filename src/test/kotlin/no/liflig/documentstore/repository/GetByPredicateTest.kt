package no.liflig.documentstore.repository

import io.kotest.matchers.shouldBe
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.OrderBy
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepo
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GetByPredicateTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
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
    assertEquals(result[0].data.text, entity1.text)
    assertEquals(result[1].data.text, entity2.text)
  }

  /**
   * Test case for testing the orderBy/orderDesc/nullsFirst/handleJsonNullsInOrderBy parameters of
   * getByPredicate.
   */
  data class OrderByTest(val orderDesc: Boolean, val nullsFirst: Boolean)

  fun orderByTestCases() =
      listOf(
          OrderByTest(orderDesc = true, nullsFirst = true),
          OrderByTest(orderDesc = true, nullsFirst = false),
          OrderByTest(orderDesc = false, nullsFirst = true),
          OrderByTest(orderDesc = false, nullsFirst = false),
      )

  @ParameterizedTest
  @MethodSource("orderByTestCases")
  fun `orderBy orders by correct data`(test: OrderByTest) {
    val entityA = ExampleEntity(text = "test", optionalText = "A")
    val entityB = ExampleEntity(text = "test", optionalText = "B")
    val entityNull = ExampleEntity(text = "test", optionalText = null)
    exampleRepo.batchCreate(listOf(entityA, entityB, entityNull))

    val result =
        exampleRepo
            .search(
                orderBy = OrderBy.OPTIONAL_TEXT,
                orderDesc = test.orderDesc,
                nullsFirst = test.nullsFirst,
                handleJsonNullsInOrderBy = true,
            )
            .map { it.data }

    val indexOfA = result.indexOf(entityA)
    val indexOfB = result.indexOf(entityB)
    val indexOfNull = result.indexOf(entityNull)

    if (test.orderDesc) {
      assert(indexOfA > indexOfB)
    } else {
      assert(indexOfA < indexOfB)
    }

    if (test.nullsFirst) {
      assert(indexOfNull < indexOfA)
      assert(indexOfNull < indexOfB)
    } else {
      assert(indexOfNull > indexOfA)
      assert(indexOfNull > indexOfB)
    }
  }

  @Test
  fun `getByPredicate should order by created_at by default`() {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "A"))
    val (entity2, _) = exampleRepo.create(ExampleEntity(text = "B"))

    val result = exampleRepo.search(orderDesc = false).map { it.data }

    val indexOf1 = result.indexOf(entity1)
    assertNotEquals(-1, indexOf1)

    val indexOf2 = result.indexOf(entity2)
    assertNotEquals(-1, indexOf2)

    assert(indexOf1 < indexOf2)
  }

  /**
   * We want users to be able to set ASC/DESC in ORDER BY themselves, in order to support ORDER BY
   * on multiple columns (the `orderDesc` parameter of [RepositoryJdbi.getByPredicate] just appends
   * "DESC" at the end of the ORDER BY clause, which does not work when ordering by multiple
   * columns).
   */
  @Test
  fun `order by multiple columns works`() {
    val (entity1, _) = exampleRepo.create(ExampleEntity(text = "2", optionalText = "2"))
    val (entity2, _) = exampleRepo.create(ExampleEntity(text = "2", optionalText = "1"))
    val (entity3, _) = exampleRepo.create(ExampleEntity(text = "1", optionalText = "1"))

    val result =
        exampleRepo.search(orderByString = "data->>'text' DESC, data->>'optionalText' DESC").map {
          it.data
        }
    result.shouldBe(listOf(entity1, entity2, entity3))
  }

  @Test
  fun `test getByPredicateWithTotalCount`() {
    val entity1 = ExampleEntity(text = "Very specific name for text query test 1")
    exampleRepo.create(entity1)
    exampleRepo.create(ExampleEntity(text = "Very specific name for text query test 2"))
    exampleRepo.create(ExampleEntity(text = "Other entity"))

    val limitLessThanCount = 2
    val result1 = exampleRepo.searchWithTotalCount(limit = limitLessThanCount)
    assertEquals(result1.list.size, limitLessThanCount)
    assertEquals(result1.totalCount, 3)

    val offsetHigherThanCount = 1000
    val result2 = exampleRepo.searchWithTotalCount(offset = offsetHigherThanCount)
    assertEquals(result2.totalCount, 3)

    val result3 =
        exampleRepo.searchWithTotalCount(
            text = "Very specific name for text query test",
            limit = 1,
            offset = 0,
            orderBy = OrderBy.TEXT,
        )
    assertEquals(result3.list.size, 1)
    assertEquals(result3.totalCount, 2)
    assertEquals(result3.list[0].data.text, entity1.text)
  }
}
