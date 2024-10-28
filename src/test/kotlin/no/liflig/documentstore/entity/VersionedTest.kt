package no.liflig.documentstore.entity

import java.time.Instant
import kotlin.test.assertEquals
import no.liflig.documentstore.testutils.ExampleEntity
import org.junit.jupiter.api.Test

class VersionedTest {
  @Test
  fun `Versioned map works as expected`() {
    val original =
        Versioned(
            ExampleEntity(text = "Initial text"),
            version = Version.initial(),
            createdAt = Instant.now(),
            modifiedAt = Instant.now(),
        )

    val mapped = original.map { it.copy(text = "New text") }

    assertEquals("New text", mapped.item.text)
    assertEquals(original.version, mapped.version)
    assertEquals(original.createdAt, mapped.createdAt)
    assertEquals(original.modifiedAt, mapped.modifiedAt)
  }
}
