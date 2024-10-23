package no.liflig.documentstore.utils

/**
 * An Iterable may or may not have a known size. But in some of our methods on RepositoryJdbi, we
 * can make optimizations, such as returning early or pre-allocating results, if we know the size.
 * So we use this extension function to see if we can get a size from the Iterable.
 *
 * This is the same way that the Kotlin standard library does it for e.g. [Iterable.map].
 */
internal fun Iterable<*>.sizeIfKnown(): Int? {
  return if (this is Collection<*>) this.size else null
}

internal fun Iterable<*>.isEmpty(): Boolean {
  // Don't constructor an Iterator if we don't have to - this is also what Apache Commons does:
  // https://github.com/apache/commons-collections/blob/554f1a6aeb883066ed6bb06cfce32d286f11d5c0/src/main/java/org/apache/commons/collections4/IterableUtils.java#L596-L601
  if (this is Collection<*>) {
    return this.isEmpty()
  }
  return !this.iterator().hasNext()
}
