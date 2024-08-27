package no.liflig.documentstore.entity

import java.time.Instant

@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
interface EntityTimestamps {
  /** Timestamp created. */
  val createdAt: Instant

  /** Timestamp last modified, including the initial creation timestamp. */
  val modifiedAt: Instant
}
