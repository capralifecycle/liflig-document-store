package no.liflig.documentstore.dao

import no.liflig.documentstore.entity.EntityRoot

@Deprecated(
    "Package location changed.",
    ReplaceWith(
        "SerializationAdapter<A>",
        imports = ["no.liflig.documentstore.repository.SerializationAdapter"],
    ),
    level = DeprecationLevel.WARNING,
)
interface SerializationAdapter<A> where A : EntityRoot<*> {
  fun toJson(entity: A): String
  fun fromJson(value: String): A
}
