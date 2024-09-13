package no.liflig.documentstore.testutils.examples

import java.time.Instant
import java.util.UUID
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

internal object InstantSerializer : KSerializer<Instant> {
  override val descriptor: SerialDescriptor =
      PrimitiveSerialDescriptor("java.time.Instant", PrimitiveKind.STRING)

  override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())

  override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

internal object UUIDSerializer : KSerializer<UUID> {
  override val descriptor: SerialDescriptor =
      PrimitiveSerialDescriptor("java.util.UUID", PrimitiveKind.STRING)

  override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())

  override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
}
