@file:UseSerializers(InstantSerializer::class, UuidSerializer::class)

package no.liflig.documentstore.examples

import java.time.Instant
import java.util.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.AbstractEntityRoot
import no.liflig.documentstore.entity.EntityTimestamps
import no.liflig.documentstore.entity.UuidEntityId

@Serializable
data class ExampleEntity(
  override val id: ExampleId = ExampleId(),
  override val createdAt: Instant = Instant.now(),
  override val modifiedAt: Instant = Instant.now(),
  val text: String,
  val moreText: String? = null,
) : AbstractEntityRoot<ExampleId>(), EntityTimestamps {
  fun update(
    text: String = this.text,
    moreText: String? = this.moreText,
  ): ExampleEntity =
      copy(
          modifiedAt = Instant.now(),
          text = text,
          moreText = moreText,
      )
}

@Serializable
@JvmInline
value class ExampleId(override val value: UUID = UUID.randomUUID()) : UuidEntityId {
  override fun toString(): String = value.toString()
}
