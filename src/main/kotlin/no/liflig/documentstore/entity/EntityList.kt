@file:Suppress("unused")

package no.liflig.documentstore.entity

import no.liflig.documentstore.dao.ListWithTotalCount

/**
 * Type alias to avoid repeated long `List<VersionedEntity<...>>` type declarations.
 *
 * Provides extension functions for mapping/filtering/iterating while keeping the same [Version]
 * (typically needed when the entity will be used for updating later).
 */
typealias EntityList<T> = List<VersionedEntity<T>>

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
fun <T : Entity<*>> EntityList<T>.mapEntities(mapFn: (T) -> T): EntityList<T> {
  return map { (entity, version) ->
    val newEntity = mapFn(entity)
    VersionedEntity(newEntity, version)
  }
}

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 * Discards entities that return null from the given map function.
 */
fun <T : Entity<*>> EntityList<T>.mapEntitiesNotNull(mapFn: (T) -> T?): EntityList<T> {
  return mapNotNull { (entity, version) ->
    val newEntity = mapFn(entity) ?: return@mapNotNull null
    VersionedEntity(newEntity, version)
  }
}

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
fun <T : Entity<*>> EntityList<T>.filterEntities(predicate: (T) -> Boolean): EntityList<T> {
  return filter { (entity, _) -> predicate(entity) }
}

/**
 * Utility function for iterating over a list of entities, when you don't care about their versions.
 */
fun <T : Entity<*>> EntityList<T>.forEachEntity(action: (T) -> Unit) {
  for ((entity, _) in this) {
    action(entity)
  }
}

/**
 * Type alias to avoid repeated long ListWithTotalCount<VersionedEntity<...>> type declarations.
 *
 * Provides utility functions for mapping/filtering/iterating while keeping the same version on the
 * VersionedEntity (typically needed when the entity will be used for updating later), and the same
 * totalCount.
 */
typealias EntityListWithTotalCount<T> = ListWithTotalCount<VersionedEntity<T>>

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
fun <T : Entity<*>> EntityListWithTotalCount<T>.mapEntities(mapFn: (T) -> T) =
    EntityListWithTotalCount(
        list = this.list.mapEntities(mapFn),
        totalCount = this.totalCount,
    )

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 * Discards entities that return null from the given map function.
 */
fun <T : Entity<*>> EntityListWithTotalCount<T>.mapEntitiesNotNull(mapFn: (T) -> T?) =
    EntityListWithTotalCount(
        list = this.list.mapEntitiesNotNull(mapFn),
        totalCount = this.totalCount,
    )

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
fun <T : Entity<*>> EntityListWithTotalCount<T>.filterEntities(predicate: (T) -> Boolean) =
    EntityListWithTotalCount(
        list = this.list.filterEntities(predicate),
        totalCount = this.totalCount,
    )

/**
 * Utility function for iterating over a list of entities, when you don't care about their versions.
 */
fun <T : Entity<*>> EntityListWithTotalCount<T>.forEachEntity(action: (T) -> Unit) {
  for ((entity, _) in this.list) {
    action(entity)
  }
}
