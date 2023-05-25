package no.liflig.documentstore

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import kotlin.coroutines.coroutineContext

/**
 * Create one shared instance of this per connection pool.
 * @see from
 */
class CoroutineJdbiWrapper(private val jdbi: Jdbi, private val semaphore: Semaphore) {

  companion object {
    fun from(jdbi: Jdbi, connectionPoolMaximumSize: Int) =
      CoroutineJdbiWrapper(jdbi, Semaphore(permits = connectionPoolMaximumSize))
  }

  suspend fun <R> withHandle(block: suspend (handle: Handle) -> R): R = withContext(Dispatchers.IO + coroutineContext) {
    semaphore.withPermit {
      jdbi.open().use {
        block(it)
      }
    }
  }
}
