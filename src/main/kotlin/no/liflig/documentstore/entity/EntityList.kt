package no.liflig.documentstore.entity

import no.liflig.documentstore.dao.ListWithTotalCount

typealias EntityList<T> = List<VersionedEntity<T>>

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityList<T>.mapEntities(mapFn: (T) -> T): EntityList<T> {
  // Initialize new list with capacity equal to previous list's size, to avoid extra allocations
  val newList = ArrayList<VersionedEntity<T>>(this.size)
  for ((entity, version) in this) {
    newList.add(VersionedEntity(mapFn(entity), version))
  }
  return newList
}

/** Same as [mapEntities], but discards entities that return null from the map function. */
fun <T : EntityRoot<*>> EntityList<T>.mapEntitiesNotNull(mapFn: (T) -> T?): EntityList<T> {
  // Not setting initial capacity here, since new list may be smaller than previous
  val newList = ArrayList<VersionedEntity<T>>()
  for ((entity, version) in this) {
    val mapped = mapFn(entity)
    if (mapped == null) {
      continue
    } else {
      newList.add(VersionedEntity(mapped, version))
    }
  }
  return newList
}

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityList<T>.filterEntities(predicate: (T) -> Boolean): EntityList<T> {
  // Not setting initial capacity here, since new list may be smaller than previous
  val newList = ArrayList<VersionedEntity<T>>()
  for (entity in this) {
    if (predicate(entity.item)) {
      newList.add(entity)
    }
  }
  return newList
}

typealias EntityListWithTotalCount<T> = ListWithTotalCount<VersionedEntity<T>>

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityListWithTotalCount<T>.mapEntities(mapFn: (T) -> T) =
    EntityListWithTotalCount(
        list = this.list.mapEntities(mapFn),
        totalCount = this.totalCount,
    )

/** Same as [mapEntities], but discards entities that return null from the map function. */
fun <T : EntityRoot<*>> EntityListWithTotalCount<T>.mapEntitiesNotNull(mapFn: (T) -> T?) =
    EntityListWithTotalCount(
        list = this.list.mapEntitiesNotNull(mapFn),
        totalCount = this.totalCount,
    )

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityListWithTotalCount<T>.filterEntities(predicate: (T) -> Boolean) =
    EntityListWithTotalCount(
        list = this.list.filterEntities(predicate),
        totalCount = this.totalCount,
    )
