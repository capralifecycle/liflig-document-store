package no.liflig.documentstore.repository

import java.sql.SQLException
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
 * Starts a transaction, and stores the database handle in a thread-local. Calls to [useHandle]
 * inside the given [block] will then use this transaction handle (this includes the various methods
 * on [RepositoryJdbi]).
 *
 * If a transaction is already in progress on the current thread, a new one will not be started
 * (since we're already in a transaction).
 */
inline fun <ReturnT> transactional(jdbi: Jdbi, block: () -> ReturnT): ReturnT {
  val existingHandle = transactionHandle.get()
  if (existingHandle != null) {
    // This means we're already in a transaction, so we do not start a new one
    return block()
  }

  val handle: Handle =
      try {
        jdbi.open()
      } catch (e: Exception) {
        throw mapDatabaseException(e)
      }

  handle.use {
    try {
      transactionHandle.set(handle)

      var attempts = 0
      val failures = ArrayList<Throwable>()

      retryLoop@ while (true) {
        try {
          /**
           * 1. Begin transaction
           * 2. Call [block]
           * 3. If it returns successfully: Commit transaction
           *     - Since we're in an inline method and `block` may contain non-local returns, we
           *       must call commit in the `finally` clause below, to ensure that it's called.
           * 4. If it throws: Roll back transaction
           *
           * We manually call begin, commit and rollback here instead of using
           * [Handle.inTransaction]. This is because we want this function to be inline, so that
           * callers can use non-local returns in `block`, which is quite handy for these types of
           * functions. `inTransaction` takes a Java lambda, which Kotlin cannot inline, hence we
           * need to do it ourselves.
           *
           * The implementation here is the same as JDBI's
           * [org.jdbi.v3.core.transaction.LocalTransactionHandler.BoundLocalTransactionHandler.inTransaction].
           */
          handle.begin()
          return block()
        } catch (e: Throwable) {
          try {
            handle.rollback()
          } catch (rollback: Exception) {
            // If rollback failed, we still want to throw the original exception, so we add this
            // exception as a suppressed exception here. This is the same as JDBI does in their
            // implementation of `inTransaction`.
            e.addSuppressed(rollback)
          }

          attempts++
          if (attempts < MAX_TRANSACTION_RETRIES && isTransactionSerializationFailure(e)) {
            failures.add(e)
            continue@retryLoop
          }

          if (failures.isNotEmpty()) {
            for (failure in failures.asReversed()) {
              e.addSuppressed(failure)
            }
          }
          throw mapDatabaseException(e)
        }
      }
    } finally {
      transactionHandle.remove()

      // If we're still in a transaction, that means we haven't rolled back, which means `block`
      // returned successfully, so we should commit.
      // See comment on `handle.begin()` above for why we call this here.
      if (handle.isInTransaction) {
        handle.commit()
      }
    }
  }
}

@kotlin.concurrent.Volatile @PublishedApi internal var MAX_TRANSACTION_RETRIES = 3

fun setMaxTransactionRetries(retries: Int) {
  MAX_TRANSACTION_RETRIES = retries
}

private const val POSTGRES_TRANSACTION_SERIALIZATION_ERROR_CODE = "40001"

/**
 * Checks if the given exception, or any of its cause exceptions, is a Postgres transaction
 * serialization failure. Such failures may occur when there's a conflict on a resource accessed in
 * a transaction, and can typically be remedied by retrying.
 */
@PublishedApi
internal fun isTransactionSerializationFailure(e: Throwable): Boolean {
  var e: Throwable? = e

  while (e != null) {
    if (e is SQLException && e.sqlState.startsWith(POSTGRES_TRANSACTION_SERIALIZATION_ERROR_CODE)) {
      return true
    }
    e = e.cause
  }

  return false
}
