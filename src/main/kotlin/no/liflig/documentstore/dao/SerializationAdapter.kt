package no.liflig.documentstore.dao

import no.liflig.documentstore.entity.EntityRoot

interface SerializationAdapter<A> where
A : EntityRoot<*> {
  fun toJson(aggregate: A): String
  fun fromJson(value: String): A
}
