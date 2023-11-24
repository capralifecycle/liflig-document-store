package no.liflig.documentstore.entity

import java.sql.Types
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry

/** Entity ID without known implementation. */
@Suppress("DataClassPrivateConstructor")
data class UnmappedEntityId private constructor(val value: String) {
  override fun toString() = value

  companion object {
    private val validPattern = Regex("[a-zA-Z0-9:\\-]+")

    fun fromString(value: String): UnmappedEntityId =
        try {
          // Some simple safeguards.
          check(value.length < 60)
          check(validPattern.matches(value))
          UnmappedEntityId(value)
        } catch (e: Exception) {
          throw IllegalArgumentException("Mapping failed", e)
        }
  }
}

fun EntityId.toUnmapped(): UnmappedEntityId = UnmappedEntityId.fromString(toString())

/** An argument factory for JDBI so that we can use a [UnmappedEntityId] as a bind argument. */
class UnmappedEntityIdArgumentFactory : AbstractArgumentFactory<UnmappedEntityId>(Types.OTHER) {
  override fun build(value: UnmappedEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.toString())
      }
}
