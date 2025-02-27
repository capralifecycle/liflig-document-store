package no.liflig.documentstore.utils

internal fun <T> arrayListWithCapacity(capacity: Int?): ArrayList<T> {
  return if (capacity != null) {
    ArrayList(capacity)
  } else {
    ArrayList()
  }
}
