package no.liflig.documentstore

import java.sql.Types
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
 * - [Version]
 */
class DocumentStorePlugin : JdbiPlugin.Singleton() {
  override fun customizeJdbi(jdbi: Jdbi) {
    jdbi
        .registerArgument(UuidEntityIdArgumentFactory())
        .registerArgument(StringEntityIdArgumentFactory())
        .registerArgument(VersionArgumentFactory())
        .registerArrayType(UuidEntityId::class.java, "uuid")
        .registerArrayType(StringEntityId::class.java, "text")
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

/** JDBI argument factory that lets us use [Version] as a bind argument. */
private class VersionArgumentFactory : AbstractArgumentFactory<Version>(Types.OTHER) {
  override fun build(value: Version, config: ConfigRegistry?): Argument =
      Argument { position, statement, _ ->
        statement.setObject(position, value.value)
      }
}

/**
 * Utility function for registering all the JDBI argument factories needed by Liflig Document Store:
 * - [UuidEntityIdArgumentFactory]
 * - [StringEntityIdArgumentFactory]
 * - [VersionArgumentFactory]
 */
@Deprecated(
    "Replaced by DocumentStorePlugin.",
    ReplaceWith(
        "installPlugin(DocumentStorePlugin())",
        imports = ["no.liflig.documentstore.DocumentStorePlugin"],
    ),
    DeprecationLevel.WARNING,
)
fun Jdbi.registerLifligArgumentTypes(): Jdbi {
  return this.installPlugin(DocumentStorePlugin())
}
