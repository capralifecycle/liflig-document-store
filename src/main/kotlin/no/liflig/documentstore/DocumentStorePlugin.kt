package no.liflig.documentstore

import no.liflig.documentstore.entity.StringEntityId
import no.liflig.documentstore.entity.StringEntityIdArgumentFactory
import no.liflig.documentstore.entity.UuidEntityId
import no.liflig.documentstore.entity.UuidEntityIdArgumentFactory
import no.liflig.documentstore.entity.VersionArgumentFactory
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.spi.JdbiPlugin

/**
 * JDBI plugin for registering all the argument types used by Liflig Document Store:
 * - [UuidEntityIdArgumentFactory]
 * - [StringEntityIdArgumentFactory]
 * - [VersionArgumentFactory]
 *
 * Usage:
 * ```
 * jdbi.installPlugin(DocumentStorePlugin())
 * ```
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
