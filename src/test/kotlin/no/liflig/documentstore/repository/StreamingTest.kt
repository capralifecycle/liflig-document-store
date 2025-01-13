package no.liflig.documentstore.repository

import io.kotest.matchers.shouldBe
import java.text.DecimalFormat
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.clearTable
import no.liflig.documentstore.testutils.exampleRepoForStreaming
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamingTest {
  @BeforeEach
  fun reset() {
    clearTable("example_for_streaming")
  }

  // For deterministic sorting
  private val numberFormat = DecimalFormat("00000")

  @Test
  fun `streamAll works`() {
    val existingEntities =
        (0 until 10_000)
            .asSequence()
            .map { number -> ExampleEntity(text = numberFormat.format(number)) }
            .asIterable()
    exampleRepoForStreaming.batchCreate(existingEntities)

    var count = 0
    exampleRepoForStreaming.streamAll { stream ->
      for (entity in stream) {
        entity.item.text shouldBe numberFormat.format(count)
        count++
      }
    }

    count shouldBe 10_000
  }

  @Test
  fun `streamByPredicate works`() {
    val existingEntities =
        (0 until 10_000)
            .asSequence()
            .map { number ->
              val text = if (number % 2 == 0) "Even" else "Odd"
              ExampleEntity(text = text)
            }
            .asIterable()
    exampleRepoForStreaming.batchCreate(existingEntities)

    var count = 0
    exampleRepoForStreaming.streamingSearch(text = "Even") { stream ->
      for (entity in stream) {
        entity.item.text shouldBe "Even"
        count++
      }
    }

    count shouldBe 5_000
  }
}
