package no.liflig.documentstore.repository

import io.kotest.matchers.shouldBe
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.exampleRepo
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CountTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
  }

  @Test
  fun `countAll returns count of all entities in table`() {
    val expectedCount = 50
    exampleRepo.batchCreate(
        (1..expectedCount).map { number -> ExampleEntity(text = number.toString()) })

    val count = exampleRepo.countAll()
    count shouldBe expectedCount
  }

  @Test
  fun `countByPredicate returns count matching where clause`() {
    val expectedCount = 25
    val testText = "Match"
    exampleRepo.batchCreate((1..expectedCount).map { ExampleEntity(text = testText) })

    exampleRepo.batchCreate((1..50).map { ExampleEntity(text = "No match") })

    val count = exampleRepo.countEntitiesWithText(testText)
    count shouldBe expectedCount
  }
}
