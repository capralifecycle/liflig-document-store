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
class ExampleEntity
private constructor(
  override val id: ExampleId,
  val text: String,
  val moreText: String?,
  override val createdAt: Instant,
  override val modifiedAt: Instant
) : AbstractEntityRoot<ExampleId>(), EntityTimestamps {
  private fun update(
    text: String = this.text,
    moreText: String? = this.moreText,
    createdAt: Instant = this.createdAt,
    modifiedAt: Instant = Instant.now()
  ): ExampleEntity =
      ExampleEntity(
          id = this.id,
          text = text,
          moreText = moreText,
          createdAt = createdAt,
          modifiedAt = modifiedAt,
      )

  fun updateText(
    text: String = this.text,
    moreText: String? = this.moreText,
  ): ExampleEntity = update(text = text, moreText = moreText)

  companion object {
    fun create(
      text: String,
      moreText: String? = null,
      now: Instant = Instant.now(),
      id: ExampleId = ExampleId()
    ): ExampleEntity =
        ExampleEntity(id = id, text = text, moreText = moreText, createdAt = now, modifiedAt = now)
  }
}

@Serializable
@JvmInline
value class ExampleId(override val value: UUID = UUID.randomUUID()) : UuidEntityId {
  override fun toString(): String = value.toString()
}
