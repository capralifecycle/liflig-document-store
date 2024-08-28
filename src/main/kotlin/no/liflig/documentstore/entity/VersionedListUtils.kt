@file:Suppress("unused")

package no.liflig.documentstore.entity

import no.liflig.documentstore.repository.ListWithTotalCount

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
inline fun <EntityT : Entity<*>> List<Versioned<EntityT>>.mapEntities(
    mapper: (EntityT) -> EntityT
): List<Versioned<EntityT>> {
  return map { entity -> entity.copy(item = mapper(entity.item)) }
}

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 * Discards entities that return null from the given map function.
 */
inline fun <EntityT : Entity<*>> List<Versioned<EntityT>>.mapEntitiesNotNull(
    mapper: (EntityT) -> EntityT?
): List<Versioned<EntityT>> {
  return mapNotNull { entity ->
    val mappedEntity = mapper(entity.item) ?: return@mapNotNull null
    entity.copy(item = mappedEntity)
  }
}

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
inline fun <EntityT : Entity<*>> List<Versioned<EntityT>>.filterEntities(
    predicate: (EntityT) -> Boolean
): List<Versioned<EntityT>> {
  return filter { entity -> predicate(entity.item) }
}

/**
 * Utility function for iterating over a list of entities, when you don't care about their versions.
 */
inline fun <EntityT : Entity<*>> List<Versioned<EntityT>>.forEachEntity(action: (EntityT) -> Unit) {
  forEach { entity -> action(entity.item) }
}

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element
 * and the same total count.
 */
inline fun <EntityT : Entity<*>> ListWithTotalCount<Versioned<EntityT>>.mapEntities(
    mapper: (EntityT) -> EntityT
): ListWithTotalCount<Versioned<EntityT>> {
  return this.copy(list = this.list.mapEntities(mapper))
}

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element
 * and the same total count. Discards entities that return null from the given map function.
 */
fun <EntityT : Entity<*>> ListWithTotalCount<Versioned<EntityT>>.mapEntitiesNotNull(
    mapper: (EntityT) -> EntityT?
): ListWithTotalCount<Versioned<EntityT>> {
  return this.copy(list = this.list.mapEntitiesNotNull(mapper))
}

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element
 * and the same total count.
 */
fun <EntityT : Entity<*>> ListWithTotalCount<Versioned<EntityT>>.filterEntities(
    predicate: (EntityT) -> Boolean
): ListWithTotalCount<Versioned<EntityT>> {
  return this.copy(list = this.list.filterEntities(predicate))
}

/**
 * Utility function for iterating over a list of entities, when you don't care about their versions.
 */
fun <EntityT : Entity<*>> ListWithTotalCount<Versioned<EntityT>>.forEachEntity(
    action: (EntityT) -> Unit
) {
  this.list.forEachEntity(action)
}
