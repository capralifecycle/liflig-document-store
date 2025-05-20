package no.liflig.documentstore.repository

import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.entity.entityIdValueToString

/** Interface for interacting with entities in a database table. */
interface Repository<EntityIdT : EntityId, EntityT : Entity<EntityIdT>> {
  /** Stores the given entity in the database. */
  fun create(entity: EntityT): Versioned<EntityT>

  /**
   * Returns the entity with the given ID from the database, or null if not found.
   *
   * @param forUpdate Set this to true to lock the entity's row in the database until a subsequent
   *   call to [update]/[delete], preventing concurrent modification. This only works when done
   *   inside a transaction (see [transactional]).
   */
  fun get(id: EntityIdT, forUpdate: Boolean = false): Versioned<EntityT>?

  /**
   * Sometimes, we always expect to find an entity for a given ID - e.g. if we've stored the ID as a
   * "foreign key" on one entity that points to another. In such cases, one may want to treat the
   * entity not being found as an exception. This method does that for you, throwing
   * [EntityNotFoundException] if [get] returns `null`, with a descriptive message that includes the
   * entity ID and repository class name.
   *
   * @param forUpdate Set this to true to lock the entity's row in the database until a subsequent
   *   call to [update]/[delete], preventing concurrent modification. This only works when done
   *   inside a transaction (see [transactional]).
   * @throws EntityNotFoundException If no entity was found with the given ID.
   */
  fun getOrThrow(id: EntityIdT, forUpdate: Boolean = false): Versioned<EntityT> {
    // We implement this here on the interface instead of on `RepositoryJdbi`, since this default
    // implementation will be the same for all repositories
    return get(id, forUpdate = forUpdate)
        ?: throw EntityNotFoundException(
            "Failed to find entity with ID '${entityIdValueToString(id)}' in database (${this::class.simpleName})",
        )
  }

  /**
   * Updates the given entity in the database, taking the previous [Version] of the entity for
   * optimistic locking: if we have retrieved an entity and then try to update it, but someone else
   * modified the entity in the meantime, a [ConflictRepositoryException] will be thrown.
   *
   * Uses a generic argument, so that a sub-type can be passed in and be returned as its proper
   * type.
   *
   * @throws ConflictRepositoryException If [previousVersion] does not match the version of the
   *   entity in the database.
   */
  fun <EntityOrSubClassT : EntityT> update(
      entity: EntityOrSubClassT,
      previousVersion: Version,
  ): Versioned<EntityOrSubClassT>

  /**
   * Utility overload of [update][Repository.update], allowing you to pass a [Versioned] instance
   * instead of a separate version argument.
   *
   * Example usage:
   * ```
   * val entity = exampleRepo.get(exampleId) ?: return null
   *
   * exampleRepo.update(entity.map { it.copy(name = "New name") })
   * ```
   *
   * @throws ConflictRepositoryException If `entity.version` does not match the version of the
   *   entity in the database.
   */
  fun <EntityOrSubClassT : EntityT> update(
      entity: Versioned<EntityOrSubClassT>
  ): Versioned<EntityOrSubClassT> {
    return update(entity.item, entity.version)
  }

  /**
   * Deletes the entity with the given ID, taking the previous [Version] of the entity for
   * optimistic locking: if we have retrieved an entity and then try to delete it, but someone else
   * modified the entity in the meantime, a [ConflictRepositoryException] will be thrown.
   *
   * @throws ConflictRepositoryException If [previousVersion] does not match the version of the
   *   entity in the database.
   */
  fun delete(id: EntityIdT, previousVersion: Version)

  /**
   * Utility overload of [delete][Repository.delete], allowing you to pass a [Versioned] instance
   * instead of a separate version argument.
   *
   * @throws ConflictRepositoryException If `entity.version` does not match the version of the
   *   entity in the database.
   */
  fun delete(entity: Versioned<EntityT>) {
    return delete(entity.item.id, entity.version)
  }

  /**
   * Returns entities from the database matching the given list of IDs.
   *
   * @param forUpdate Set this to true to lock the rows of the returned entities in the database
   *   until a subsequent call to [update]/[delete], preventing concurrent modification. This only
   *   works when done inside a transaction (see [transactional]).
   */
  fun listByIds(ids: List<EntityIdT>, forUpdate: Boolean = false): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    return ids.mapNotNull { id -> get(id) }
  }

  /**
   * Returns a list of all entities in the database table.
   *
   * If the table has a large number of entities and you don't want to keep them all in memory at
   * the same time, you should consider using [RepositoryJdbi.streamAll] instead.
   */
  fun listAll(): List<Versioned<EntityT>>

  /** Returns the total count of entities in the database table. */
  fun countAll(): Long {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    return listAll().size.toLong()
  }

  /**
   * Stores the given entities in the database.
   *
   * The implementation in [RepositoryJdbi.batchCreate] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   */
  fun batchCreate(entities: Iterable<EntityT>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { create(it) }
  }

  /**
   * Overload of [batchCreate][Repository.batchCreate] that takes an [Iterator] instead of an
   * [Iterable], and does not return the created entities. Use this if you're storing a large batch,
   * e.g. from a database stream, and you don't need results returned.
   */
  fun batchCreate(entities: Iterator<EntityT>) {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    entities.forEach { create(it) }
  }

  /**
   * Updates the given entities in the database. Takes the previous versions of the entities, to
   * implement optimistic locking like [update].
   *
   * The implementation in [RepositoryJdbi.batchUpdate] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchUpdate(entities: Iterable<Versioned<EntityT>>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { update(it) }
  }

  /**
   * Overload of [batchUpdate][Repository.batchUpdate] that takes an [Iterator] instead of an
   * [Iterable], and does not return the created entities. Use this if you're updating a large
   * batch, e.g. from a database stream, and you don't need results returned.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchUpdate(entities: Iterator<Versioned<EntityT>>) {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    entities.forEach { update(it) }
  }

  /**
   * Deletes the given entities from the database. Takes the previous versions of the entities, to
   * implement optimistic locking like [delete].
   *
   * The implementation in [RepositoryJdbi.batchDelete] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchDelete(entities: Iterable<Versioned<EntityT>>) {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    entities.forEach { delete(it) }
  }

  /**
   * Overload of [batchDelete][Repository.batchDelete] that takes an [Iterator] instead of an
   * [Iterable].
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchDelete(entities: Iterator<Versioned<EntityT>>) {
    // A default implementation is provided here on the interface, so that implementers don't have
    // to implement this themselves (for e.g. mock repositories).
    entities.forEach { delete(it) }
  }
}
