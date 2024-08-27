package no.liflig.documentstore.entity

interface Entity<EntityIdT : EntityId> {
  val id: EntityIdT
}

@Deprecated(
    "This interface was a redundant abstraction on top of the Entity interface, so we're replacing this with just the Entity interface.",
    ReplaceWith(
        "Entity<EntityIdT>",
        imports = ["no.liflig.documentstore.entity.Entity"],
    ),
    level = DeprecationLevel.WARNING,
)
interface EntityRoot<EntityIdT : EntityId> : Entity<EntityIdT>

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

/** Base class for the root Entity. */
@Deprecated(
    "This class was a redundant abstraction on top of AbstractEntity, so we're replacing this with just AbstractEntity.",
    ReplaceWith(
        "AbstractEntity<EntityIdT>",
        imports = ["no.liflig.documentstore.entity.AbstractEntity"],
    ),
    level = DeprecationLevel.WARNING,
)
abstract class AbstractEntityRoot<EntityIdT : EntityId> :
    AbstractEntity<EntityIdT>(), EntityRoot<EntityIdT>
