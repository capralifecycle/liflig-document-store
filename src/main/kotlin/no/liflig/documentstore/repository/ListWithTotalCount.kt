package no.liflig.documentstore.repository

/**
 * A list of items that are part of a larger collection, and the total count of items in that
 * collection. For example, a paginated list of database entities, and the total count of entities
 * in the table.
 *
 * Returned by [RepositoryJdbi.getByPredicateWithTotalCount], which can be used to show the total
 * number of pages for a collection that uses pagination.
 */
data class ListWithTotalCount<T>(
    val list: List<T>,
    val totalCount: Long,
) {
  /** Maps the elements of the list, while keeping the same [totalCount]. */
  fun <R> map(transform: (T) -> R): ListWithTotalCount<R> {
    return ListWithTotalCount(
        list = this.list.map(transform),
        totalCount = this.totalCount,
    )
  }
}
