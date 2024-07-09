package no.liflig.documentstore

import no.liflig.documentstore.entity.StringEntityId
import no.liflig.documentstore.entity.StringEntityIdArgumentFactory
import no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory
import no.liflig.documentstore.entity.UuidEntityId
import no.liflig.documentstore.entity.UuidEntityIdArgumentFactory
import no.liflig.documentstore.entity.VersionArgumentFactory
import org.jdbi.v3.core.Jdbi

fun Jdbi.registerLifligArgumentTypes(): Jdbi {
  return this.registerArgument(UuidEntityIdArgumentFactory())
      .registerArgument(StringEntityIdArgumentFactory())
      .registerArgument(UnmappedEntityIdArgumentFactory())
      .registerArgument(VersionArgumentFactory())
      .registerArrayType(UuidEntityId::class.java, "uuid")
      .registerArrayType(StringEntityId::class.java, "text")
}
