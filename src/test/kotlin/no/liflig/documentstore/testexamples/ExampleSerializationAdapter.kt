package no.liflig.documentstore.testexamples

import kotlinx.serialization.json.Json
import no.liflig.documentstore.dao.SerializationAdapter

class ExampleSerializationAdapter : SerializationAdapter<ExampleEntity> {

  val json: Json = Json {
    encodeDefaults = true
    ignoreUnknownKeys = true
  }

  val serializer = ExampleEntity.serializer()

  override fun toJson(entity: ExampleEntity): String = json.encodeToString(serializer, entity)

  override fun fromJson(value: String): ExampleEntity = json.decodeFromString(serializer, value)
}
