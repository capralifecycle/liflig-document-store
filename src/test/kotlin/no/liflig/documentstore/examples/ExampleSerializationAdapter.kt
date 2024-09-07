package no.liflig.documentstore.examples

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.repository.SerializationAdapter

private val json = Json {
  encodeDefaults = true
  ignoreUnknownKeys = true
}

class KotlinSerialization<EntityT : Entity<*>>(
    private val serializer: KSerializer<EntityT>,
) : SerializationAdapter<EntityT> {
  override fun toJson(entity: EntityT): String = json.encodeToString(serializer, entity)

  override fun fromJson(value: String): EntityT = json.decodeFromString(serializer, value)
}
