@file:Suppress("unused") // This is a library, we want to expose these classes

package no.liflig.documentstore.entity

interface Entity<EntityIdT : EntityId> {
  val id: EntityIdT
}

/**
 * Base class for an entity when you want two entities to compare as equal if their IDs are equal.
 */
abstract class AbstractEntity<EntityIdT : EntityId> : Entity<EntityIdT> {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as AbstractEntity<*>

    return id == other.id
  }

  override fun hashCode(): Int = id.hashCode()
}
