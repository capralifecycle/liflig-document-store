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
  fun batchCreate(entities: List<EntityT>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { entity -> create(entity) }
  }

  /**
   * Takes a list of entities along with their previous [Version], to implement optimistic locking
   * like [update]. Returns the updated entities along with their new version.
   *
   * The implementation in [RepositoryJdbi.batchUpdate] uses
   * [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to make the
   * implementation as efficient as possible.
   *
   * @throws ConflictRepositoryException If the version on any of the given entities does not match
   *   the current version of the entity in the database.
   */
  fun batchUpdate(entities: List<Versioned<EntityT>>): List<Versioned<EntityT>> {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    return entities.map { entity -> update(entity.item, entity.version) }
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
  fun batchDelete(entities: List<Versioned<EntityT>>) {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    for (entity in entities) {
      delete(entity.item.id, entity.version)
    }
  }

  /**
   * When using a document store, the application must take care to stay backwards-compatible with
   * entities that are stored in the database. Thus, new fields added to entitites must always have
   * a default value, so that entities stored before the field was added can still be deserialized.
   *
   * Sometimes we may add a field with a default value, but also want to query on that field in e.g.
   * [RepositoryJdbi.getByPredicate]. In this case, it's not enough to add a default value to the
   * field that is populated on deserializing from the database - we actually have to migrate the
   * stored entity. This method exists for those cases.
   *
   * In the implementation for [RepositoryJdbi.migrate], all entities are read from the database
   * table, in a streaming fashion to avoid reading them all into memory. It then updates the
   * entities in batches (see [OPTIMAL_BATCH_SIZE]).
   *
   * It is important that the migration is done in an idempotent manner, i.e. that it may be
   * executed repeatedly with the same results. This is because we call this method from application
   * code, and if for example there are multiple instances of the service running, [migrate] will be
   * called by each one.
   *
   * Any new fields on the entity with default values will be stored in the database through the
   * process of deserializing and re-serializing here. If you want to do further transforms, you can
   * use the [transformEntity] parameter.
   */
  fun migrate(transformEntity: ((Versioned<EntityT>) -> EntityT)? = null) {
    // A default implementation is provided here on the interface, so that implementors don't have
    // to implement this themselves (for e.g. mock repositories).
    var entities = listAll()
    if (transformEntity != null) {
      entities = entities.map { entity -> entity.copy(item = transformEntity(entity)) }
    }
    batchUpdate(entities)
  }
}
