package no.liflig.documentstore.entity

import java.sql.Types
import java.util.*
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry

/** EntityId represents the ID pointing to a specific [Entity]. */
interface EntityId

/** UUID-version of an EntityId. */
interface UuidEntityId : EntityId {
  val value: UUID
}

/** An argument factory for JDBI so that we can use a [UuidEntityId] as a bind argument. */
class UuidEntityIdArgumentFactory : AbstractArgumentFactory<UuidEntityId>(Types.OTHER) {
  override fun build(value: UuidEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}
