package no.liflig.documentstore.entity

import java.sql.Types
import java.time.Instant
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry

/**
 * A [Version] is a count of how many times an entity has been modified, used to implement
 * optimistic locking.
 */
data class Version(val value: Long) {
  fun next() = Version(value + 1)

  companion object {
    fun initial() = Version(1)
  }
}

/**
 * A wrapper around a database entity, providing metadata such as the current [Version] (used for
 * optimistic locking in [Repository.update][no.liflig.documentstore.repository.Repository.update]
 * and [delete][no.liflig.documentstore.repository.Repository.delete]), when it was created and when
 * it was last modified.
 */
data class Versioned<out EntityT : Entity<*>>(
    val item: EntityT,
    val version: Version,
    val createdAt: Instant,
    val modifiedAt: Instant,
)

@Deprecated(
    "Replaced by Versioned<EntityT>, which also adds createdAt and modifiedAt fields. We renamed this because this type never appears without its corresponding entity, so VersionedEntity<Entity> was redundant and led to excessively long type signatures.",
    ReplaceWith(
        "Versioned<EntityT>",
        imports = ["no.liflig.documentstore.entity.Versioned"],
    ),
    level = DeprecationLevel.WARNING,
)
data class VersionedEntity<out EntityT : Entity<*>>(val item: EntityT, val version: Version)

/** An argument factory for JDBI so that we can use a [Version] as a bind argument. */
class VersionArgumentFactory : AbstractArgumentFactory<Version>(Types.OTHER) {
  override fun build(value: Version, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}
