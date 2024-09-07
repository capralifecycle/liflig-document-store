package no.liflig.documentstore.repository

import no.liflig.documentstore.entity.Entity

/**
 * Interface to allow users to choose their own JSON serialization library.
 *
 * Example implementation for `kotlinx.serialization`:
 * ```
 * import kotlinx.serialization.KSerializer
 * import kotlinx.serialization.json.Json
 * import no.liflig.documentstore.entity.Entity
 * import no.liflig.documentstore.repository.SerializationAdapter
 *
 * private val json = Json {
 *   encodeDefaults = true
 *   ignoreUnknownKeys = true
 * }
 *
 * class KotlinSerialization<EntityT : Entity<*>>(
 *     private val serializer: KSerializer<EntityT>,
 * ) : SerializationAdapter<EntityT> {
 *   override fun toJson(entity: EntityT): String = json.encodeToString(serializer, entity)
 *
 *   override fun fromJson(value: String): EntityT = json.decodeFromString(serializer, value)
 * }
 * ```
 *
 * Then you can pass it to [RepositoryJdbi] like this:
 * ```
 * class ExampleRepository(jdbi: Jdbi) :
 *     RepositoryJdbi<ExampleId, ExampleEntity>(
 *         jdbi,
 *         tableName = "example",
 *         KotlinSerialization(ExampleEntity.serializer()),
 *     )
 * ```
 */
interface SerializationAdapter<EntityT> where EntityT : Entity<*> {
  fun toJson(entity: EntityT): String
  fun fromJson(value: String): EntityT
}
