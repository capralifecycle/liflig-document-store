package no.liflig.documentstore

import kotlinx.serialization.json.Json
import no.liflig.documentstore.dao.SerializationAdapter

class ExampleSerializationAdapter : SerializationAdapter<ExampleAggregate> {

  val json: Json = Json {
    encodeDefaults = true
    ignoreUnknownKeys = true
  }

  val serializer = ExampleAggregate.serializer()

  override fun toJson(aggregate: ExampleAggregate): String = json.encodeToString(serializer, aggregate)

  override fun fromJson(value: String): ExampleAggregate = json.decodeFromString(serializer, value)
}
