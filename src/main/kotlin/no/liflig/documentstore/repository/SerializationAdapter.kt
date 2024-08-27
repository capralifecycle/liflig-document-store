package no.liflig.documentstore.repository

import no.liflig.documentstore.entity.Entity

/** Interface to allow users to choose their own JSON serialization library. */
interface SerializationAdapter<EntityT> where EntityT : Entity<*> {
  fun toJson(entity: EntityT): String
  fun fromJson(value: String): EntityT
}
