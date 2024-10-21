package no.liflig.documentstore.repository

import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.Versioned

/** Interface for interacting with entities in a database table. */
interface Repository<EntityIdT : EntityId, EntityT : Entity<EntityIdT>> {
  fun create(entity: EntityT): Versioned<EntityT>

  /**
   * @param forUpdate Set this to true to lock the entity's row in the database until a subsequent
   *   call to [update], preventing concurrent modification. If setting this to true, you should use
   *   a transaction for the get and update (see [transactional]).
   */
  fun get(id: EntityIdT, forUpdate: Boolean = false): Versioned<EntityT>?

  /**
   * Updates the given entity, taking the previous [Version] of the entity for optimistic locking:
   * if we have retrieved an entity and then try to update it, but someone else modified the entity
   * in the meantime, a [ConflictRepositoryException] will be thrown.
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
   * Deletes the entity with the given ID, taking the previous [Version] of the entity for
   * optimistic locking: if we have retrieved an entity and then try to delete it, but someone else
   * modified the entity in the meantime, a [ConflictRepositoryException] will be thrown.
   *
   * @throws ConflictRepositoryException If [previousVersion] does not match the version of the
   *   entity in the database.
   */
  fun delete(id: EntityIdT, previousVersion: Version)

  fun listByIds(ids: List<EntityIdT>): List<Versioned<EntityT>>

  fun listAll(): List<Versioned<EntityT>>

  /**
   * Stores the given list of entities in the database.
   *
   * The implementation in [RepositoryJdbi.batchCreate] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   */
  fun batchCreate(entities: Iterable<EntityT>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { create(it) }
  }

  /**
   * Takes a list of entities along with their previous [Version], to implement optimistic locking
   * like [update].
   *
   * The implementation in [RepositoryJdbi.batchUpdate] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchUpdate(entities: Iterable<Versioned<EntityT>>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { update(it.item, it.version) }
  }

  /**
   * Takes a list of entities along with their previous [Version], to implement optimistic locking
   * like [delete].
   *
   * The implementation in [RepositoryJdbi.batchDelete] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchDelete(entities: Iterable<Versioned<EntityT>>) {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    for (entity in entities) {
      delete(entity.item.id, entity.version)
    }
  }

  /**
   * Initiates a database transaction, and executes the given [block] inside of it. Any calls to
   * other repository methods inside this block will use the same transaction, and roll back if an
   * exception is thrown.
   *
   * The implementation in [RepositoryJdbi.transactional] uses its JDBI instance to start the
   * transaction.
   */
  fun <ReturnT> transactional(block: () -> ReturnT): ReturnT {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    return block()
  }
}
