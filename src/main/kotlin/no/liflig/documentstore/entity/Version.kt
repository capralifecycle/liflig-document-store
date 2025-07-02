package no.liflig.documentstore.entity

import java.time.Instant
import no.liflig.documentstore.entity.Version.Companion.initial

/**
 * A [Version] is a count of how many times an entity has been modified, used to implement
 * optimistic locking.
 */
data class Version(val value: Long) {
  fun next() = Version(value + 1)

  companion object {
    /** Returns version 1. */
    fun initial() = INITIAL

    /**
     * Since this class is immutable, we can use a single instance for the initial version, so we
     * don't allocate on every call to [initial].
     */
    private val INITIAL = Version(1)
  }
}

/**
 * A wrapper around a database entity, providing metadata such as the current [Version] (used for
 * optimistic locking in [Repository.update][no.liflig.documentstore.repository.Repository.update]
 * and [delete][no.liflig.documentstore.repository.Repository.delete]), when it was created and when
 * it was last modified.
 */
data class Versioned<EntityT : Entity<*>>(
    /** The deserialized entity JSON data, stored in the `data` column in the database table. */
    val data: EntityT,
    val version: Version,
    val createdAt: Instant,
    val modifiedAt: Instant,
) {
  /**
   * Applies the given transform function to the entity [data], leaving [version], [createdAt] and
   * [modifiedAt] unchanged.
   *
   * Example usage:
   * ```
   * val entity = exampleRepo.get(exampleId) ?: return null
   *
   * exampleRepo.update(entity.map { it.copy(name = "New name") })
   * ```
   */
  inline fun map(transform: (EntityT) -> EntityT): Versioned<EntityT> {
    return this.copy(data = transform(data))
  }

  /** Use [data] instead. Kept around for backwards compatibility. */
  @Deprecated("Renamed to 'data'", ReplaceWith("data"))
  @Suppress("unused")
  val item: EntityT
    get() = data
}
