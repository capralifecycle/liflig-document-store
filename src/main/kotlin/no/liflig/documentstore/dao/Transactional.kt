package no.liflig.documentstore.dao

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.lang.Exception

val transactionHandle = ThreadLocal<Handle?>()

fun <T> transactional(jdbi: Jdbi, block: (Handle) -> T): T {
  val existingHandle = transactionHandle.get()

  // We can assume that we're already in a transaction, so we just pass the existing handle into the block
  return if (existingHandle != null) {
    block(existingHandle)
  } else
    mapExceptions {
      jdbi.open().use { handle ->
        try {
          transactionHandle.set(handle)
          handle.inTransaction<T, Exception> { block(handle) }
        } finally {
          transactionHandle.remove()
        }
      }
    }
}

suspend fun <T> coTransactional(jdbi: Jdbi, block: suspend (Handle) -> T): T {
  val existingHandle = transactionHandle.get()

  // We can assume that we're already in a transaction, so we just pass the existing handle into the block
  return if (existingHandle != null) {
    block(existingHandle)
  } else
    mapExceptions {
      jdbi.open().use { handle ->
        try {
          transactionHandle.set(handle)
          handle.begin()
          block(handle).also {
            handle.commit()
          }
        } finally {
          transactionHandle.remove()
        }
      }
    }
}
