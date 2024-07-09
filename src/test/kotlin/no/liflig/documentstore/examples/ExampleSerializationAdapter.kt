package no.liflig.documentstore.examples

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.liflig.documentstore.dao.SerializationAdapter

val json: Json = Json {
  encodeDefaults = true
  ignoreUnknownKeys = true
}

object ExampleSerializationAdapter : SerializationAdapter<ExampleEntity> {
  override fun toJson(entity: ExampleEntity): String = json.encodeToString(entity)

  override fun fromJson(value: String): ExampleEntity = json.decodeFromString(value)
}

object EntityWithStringIdSerializationAdapter : SerializationAdapter<EntityWithStringId> {
  override fun toJson(entity: EntityWithStringId): String = json.encodeToString(entity)

  override fun fromJson(value: String): EntityWithStringId = json.decodeFromString(value)
}
