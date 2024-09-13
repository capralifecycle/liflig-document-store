package no.liflig.documentstore.repository

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi

// `@PublishedApi` lets us use this in inline functions. Renaming or removing this may be a breaking
// change.
@PublishedApi internal val transactionHandle = ThreadLocal<Handle?>()

/**
 * Gets a database handle either from an ongoing transaction (from [transactional]), or if none is
 * found, gets a new handle with [Jdbi.open] (automatically closed after the given [block] returns).
 *
 * You should use this function whenever you want to write custom SQL using Liflig Document Store,
 * so that your implementation plays well with transactions.
 */
inline fun <ReturnT> useHandle(jdbi: Jdbi, block: (Handle) -> ReturnT): ReturnT {
  val existingHandle = transactionHandle.get()
  if (existingHandle != null) {
    return block(existingHandle)
  }

  try {
    return jdbi.open().use(block)
  } catch (e: Exception) {
    throw mapDatabaseException(e)
  }
}

/**
 * Initiates a transaction, and stores the handle in a thread-local. Calls to [useHandle] inside the
 * given [block] will then use this transaction handle (this includes the various methods on
 * [RepositoryJdbi]).
 *
 * If a transaction is already in progress on the current thread, a new one will not be initiated.
 */
fun <ReturnT> transactional(jdbi: Jdbi, block: () -> ReturnT): ReturnT {
  val existingHandle = transactionHandle.get()
  if (existingHandle != null) {
    // We can assume that we're already in a transaction, so we do not start a new one
    return block()
  }

  try {
    jdbi.open().use { handle ->
      try {
        transactionHandle.set(handle)
        return handle.inTransaction<ReturnT, Exception> { block() }
      } finally {
        transactionHandle.remove()
      }
    }
  } catch (e: Exception) {
    throw mapDatabaseException(e)
  }
}

/**
 * Short-hand for calling [useHandle] inside [transactional], i.e. starting a transaction and
 * getting a database handle inside it, or re-using an existing transaction if one is already
 * started on this thread.
 *
 * Not public, since we only use this internally in [RepositoryJdbi] - if library users find a need
 * for this, we may consider making it public.
 */
internal fun <ReturnT> useTransactionHandle(jdbi: Jdbi, block: (Handle) -> ReturnT): ReturnT {
  return transactional(jdbi) { useHandle(jdbi, block) }
}
