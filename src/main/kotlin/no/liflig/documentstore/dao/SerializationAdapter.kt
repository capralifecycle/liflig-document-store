package no.liflig.documentstore.dao

import no.liflig.documentstore.entity.EntityRoot

interface SerializationAdapter<A> where
A : EntityRoot<*> {
  fun toJson(entity: A): String
  fun fromJson(value: String): A
}
