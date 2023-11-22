package no.liflig.documentstore.dao

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.lang.Exception

val transactionHandle = ThreadLocal<Handle?>()

/**
 * Get a Handle to the data source wrapped by this Jdbi instance, either the one that exists in [transactionHandle]
 * provided by [transactional], or one will be obtained by calling [Jdbi.open]
 */
internal fun <T> getHandle(jdbi: Jdbi, useHandle: (Handle) -> T): T {
  val transactionHandle = transactionHandle.get()

  return if (transactionHandle != null) {
    useHandle(transactionHandle)
  } else {
    mapExceptions {
      jdbi.open().use { handle ->
        useHandle(handle)
      }
    }
  }
}

/**
 * Initiates a transaction, and stores the handle in [transactionHandle].
 * If a transaction is already in progress, a new one will not be initiated.
 * [transactionHandle] will then be used by [getHandle] if called inside [block]
 */
fun <T> transactional(jdbi: Jdbi, block: () -> T): T {
  val existingHandle = transactionHandle.get()

  // We can assume that we're already in a transaction, so we do not start a new one
  return if (existingHandle != null) {
    block()
  } else
    mapExceptions {
      jdbi.open().use { handle ->
        try {
          transactionHandle.set(handle)
          handle.inTransaction<T, Exception> { block() }
        } finally {
          transactionHandle.remove()
        }
      }
    }
}

/**
 * Similar to [transactional], but supports suspend functions
 */
suspend fun <T> coTransactional(jdbi: Jdbi, block: suspend () -> T): T {
  val existingHandle = transactionHandle.get()

  // We can assume that we're already in a transaction, so we do not start a new one
  return if (existingHandle != null) {
    block()
  } else
    mapExceptions {
      jdbi.open().use { handle ->
        try {
          transactionHandle.set(handle)
          handle.begin()
          block().also {
            handle.commit()
          }
        } finally {
          transactionHandle.remove()
        }
      }
    }
}
