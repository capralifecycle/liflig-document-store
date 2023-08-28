@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.testexamples

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import no.liflig.documentstore.entity.AbstractEntityRoot
import no.liflig.documentstore.entity.EntityTimestamps
import no.liflig.documentstore.entity.UuidEntityId
import java.time.Instant
import java.util.UUID

@Serializable
class ExampleEntity private constructor(
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
      modifiedAt = modifiedAt
    )

  fun updateText(
    text: String = this.text,
    moreText: String? = this.moreText,
  ): ExampleEntity =
    update(
      text = text,
      moreText = moreText
    )

  companion object {
    fun create(
      text: String,
      moreText: String? = null,
      now: Instant = Instant.now(),
      id: ExampleId = ExampleId()
    ): ExampleEntity =
      ExampleEntity(
        id = id,
        text = text,
        moreText = moreText,
        createdAt = now,
        modifiedAt = now
      )
  }
}

abstract class UuidEntityIdSerializer<T : UuidEntityId>(
  val factory: (UUID) -> T
) : KSerializer<T> {
  override val descriptor: SerialDescriptor =
    PrimitiveSerialDescriptor("UuidEntityId", PrimitiveKind.STRING)

  override fun serialize(encoder: Encoder, value: T) =
    encoder.encodeString(value.id.toString())

  override fun deserialize(decoder: Decoder): T =
    factory(UUID.fromString(decoder.decodeString()))
}

object ExampleIdSerializer : UuidEntityIdSerializer<ExampleId>({ ExampleId(it) })

@Serializable(with = ExampleIdSerializer::class)
data class ExampleId(
  override val id: UUID = UUID.randomUUID()
) : UuidEntityId {
  override fun toString(): String = id.toString()
}
