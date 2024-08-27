package no.liflig.documentstore.examples

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.liflig.documentstore.repository.SerializationAdapter

internal val json: Json = Json {
  encodeDefaults = true
  ignoreUnknownKeys = true
}

internal object ExampleSerializationAdapter : SerializationAdapter<ExampleEntity> {
  override fun toJson(entity: ExampleEntity): String = json.encodeToString(entity)

  override fun fromJson(value: String): ExampleEntity = json.decodeFromString(value)
}

internal object EntityWithStringIdSerializationAdapter : SerializationAdapter<EntityWithStringId> {
  override fun toJson(entity: EntityWithStringId): String = json.encodeToString(entity)

  override fun fromJson(value: String): EntityWithStringId = json.decodeFromString(value)
}
