package no.liflig.documentstore.entity

import java.sql.Types
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry

/**
 * A [Version] is a count of how many times an entity has been modified, and is used to implement
 * optimistic locking.
 */
data class Version(val value: Long) {
  fun next() = Version(value + 1)

  companion object {
    fun initial() = Version(1)
  }
}

data class VersionedEntity<out EntityT : Entity<*>>(val item: EntityT, val version: Version)

/** An argument factory for JDBI so that we can use a [Version] as a bind argument. */
class VersionArgumentFactory : AbstractArgumentFactory<Version>(Types.OTHER) {
  override fun build(value: Version, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}
