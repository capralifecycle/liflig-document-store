package no.liflig.documentstore.entity

import no.liflig.documentstore.dao.ListWithTotalCount

typealias EntityList<T> = List<VersionedEntity<T>>

/**
 * Utility function for mapping a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityList<T>.mapEntities(mapFn: (T) -> T): EntityList<T> {
  return map { (entity, version) ->
    val newEntity = mapFn(entity)
    VersionedEntity(newEntity, version)
  }
}

/** Same as [mapEntities], but discards entities that return null from the map function. */
fun <T : EntityRoot<*>> EntityList<T>.mapEntitiesNotNull(mapFn: (T) -> T?): EntityList<T> {
  return mapNotNull { (entity, version) ->
    val newEntity = mapFn(entity) ?: return@mapNotNull null
    VersionedEntity(newEntity, version)
  }
}

/**
 * Utility function for filtering a list of entities, but keeping the same version for each element.
 */
fun <T : EntityRoot<*>> EntityList<T>.filterEntities(predicate: (T) -> Boolean): EntityList<T> {
  return filter { (entity, _) -> predicate(entity) }
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
