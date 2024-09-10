package no.liflig.documentstore.entity

import java.time.Instant

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
