package no.liflig.documentstore.entity

import java.sql.Types
import java.util.*
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry

/** EntityId represents the ID pointing to a specific [Entity]. */
sealed interface EntityId

/** UUID version of an [EntityId], for `id UUID` columns. */
interface UuidEntityId : EntityId {
  val value: UUID
}

/** String version of an [EntityId], for `id TEXT` columns. */
interface StringEntityId : EntityId {
  val value: String
}

/**
 * In order to use JDBI's `bindArray` method to bind list arguments, we have to supply the list's
 * element type at runtime. We use this in `RepositoryJdbi.listByIds`. However, we don't have the
 * runtime type of `EntityId` there, and we cannot use `reified` on class type parameters. Thus, we
 * use this function to get the runtime type of the `EntityId`, so we can bind it correctly.
 */
internal fun getEntityIdType(entityId: EntityId): Class<out EntityId> {
  return when (entityId) {
    is UuidEntityId -> UuidEntityId::class.java
    is StringEntityId -> StringEntityId::class.java
  }
}

/** An argument factory for JDBI so that we can use a [UuidEntityId] as a bind argument. */
class UuidEntityIdArgumentFactory : AbstractArgumentFactory<UuidEntityId>(Types.OTHER) {
  override fun build(value: UuidEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}

/** An argument factory for JDBI so that we can use a [UuidEntityId] as a bind argument. */
class StringEntityIdArgumentFactory : AbstractArgumentFactory<StringEntityId>(Types.OTHER) {
  override fun build(value: StringEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setString(position, value.value)
      }
}
