package no.liflig.documentstore.dao

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.lang.Exception

val transactionHandle = ThreadLocal<Handle?>()

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
