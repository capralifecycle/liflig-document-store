@file:Suppress("MoveLambdaOutsideParentheses")

package no.liflig.documentstore

import java.sql.Types
import no.liflig.documentstore.entity.IntegerEntityId
import no.liflig.documentstore.entity.StringEntityId
import no.liflig.documentstore.entity.UuidEntityId
import no.liflig.documentstore.entity.Version
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.argument.AbstractArgumentFactory
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.config.ConfigRegistry
import org.jdbi.v3.core.spi.JdbiPlugin

/**
 * JDBI plugin that registers all bind argument types used by Liflig Document Store. In order to use
 * Document Store, you must install this when setting up your Jdbi instance:
 * ```
 * jdbi.installPlugin(DocumentStorePlugin())
 * ```
 *
 * Once this plugin is installed, the following types can be used as JDBI bind arguments:
 * - [UuidEntityId]
 * - [StringEntityId]
 * - [IntegerEntityId]
 * - [Version]
 *
 * In addition, [UuidEntityId], [StringEntityId] and [IntegerEntityId] may be used as arguments to
 * [bindArray][org.jdbi.v3.core.statement.Query.bindArray], like this:
 * ```
 * bindArray("ids", UuidEntityId::class.java, ids) // where ids: List<T extends UuidEntityId>
 * bindArray("ids", StringEntityId::class.java, ids) // where ids: List<T extends StringEntityId>
 * bindArray("ids", IntegerEntityId::class.java, ids) // where ids: List<T extends IntegerEntityId>
 * ```
 */
class DocumentStorePlugin : JdbiPlugin.Singleton() {
  override fun customizeJdbi(jdbi: Jdbi) {
    jdbi
        .registerArgument(VersionArgumentFactory())
        .registerArgument(UuidEntityIdArgumentFactory())
        .registerArgument(StringEntityIdArgumentFactory())
        .registerArgument(IntegerEntityIdArgumentFactory())
        .registerArrayType(UuidEntityId::class.java, "uuid", { id -> id.value })
        .registerArrayType(StringEntityId::class.java, "text", { id -> id.value })
        .registerArrayType(IntegerEntityId::class.java, "bigint", { id -> id.value })
  }
}

/** JDBI argument factory that lets us use [UuidEntityId] as a bind argument. */
private class UuidEntityIdArgumentFactory : AbstractArgumentFactory<UuidEntityId>(Types.OTHER) {
  override fun build(value: UuidEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}

/** JDBI argument factory that lets us use [StringEntityId] as a bind argument. */
private class StringEntityIdArgumentFactory : AbstractArgumentFactory<StringEntityId>(Types.OTHER) {
  override fun build(value: StringEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setString(position, value.value)
      }
}

/** JDBI argument factory that lets us use [IntegerEntityId] as a bind argument. */
private class IntegerEntityIdArgumentFactory :
    AbstractArgumentFactory<IntegerEntityId>(Types.BIGINT) {
  override fun build(value: IntegerEntityId, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setLong(position, value.value)
      }
}

/** JDBI argument factory that lets us use [Version] as a bind argument. */
private class VersionArgumentFactory : AbstractArgumentFactory<Version>(Types.OTHER) {
  override fun build(value: Version, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}
