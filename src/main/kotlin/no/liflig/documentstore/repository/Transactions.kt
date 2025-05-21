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
 * Starts a database transaction, and runs the given [block] inside of it. Calls to [useHandle]
 * inside the block will use the same transaction (this includes the various methods on
 * [RepositoryJdbi]). If an exception is thrown, the transaction is rolled back.
 *
 * If a transaction is already in progress on the current thread, a new one will not be started
 * (since we're already in a transaction).
 *
 * ### Thread safety
 *
 * This function stores a transaction handle in a thread-local, so that operations within [block]
 * can get the handle. But new threads spawned in the scope of `block` will not see this
 * thread-local, and so they will not work correctly with the transaction. So you should not attempt
 * concurrent database operations with this function.
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

      /**
       * 1. Begin transaction
       * 2. Call [block]
       * 3. If it returns successfully: Commit transaction
       *     - Since we're in an inline method and `block` may contain non-local returns, we must
       *       call commit in the `finally` clause below, to ensure that it's called.
       * 4. If it throws: Roll back transaction
       *
       * We manually call begin, commit and rollback here instead of using [Handle.inTransaction].
       * This is because we want this function to be inline, so that callers can use non-local
       * returns in `block`, which is quite handy for these types of functions. `inTransaction`
       * takes a Java lambda, which Kotlin cannot inline, hence we need to do it ourselves.
       *
       * The implementation here matches JDBI's
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

      throw mapDatabaseException(e)
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

/**
 * Utility class that wraps a [Jdbi] instance to provide a [transactional] method for running
 * database transactions.
 *
 * This is useful when you want to use the top-level
 * [transactional][no.liflig.documentstore.repository.transactional] function, but you don't want to
 * pass around a `Jdbi` instance everywhere.
 *
 * If you're in the context of a repository, you should probably use [RepositoryJdbi.transactional]
 * instead. But if you're running a transaction that includes multiple repositories, you may want to
 * use this class instead to make it explicit that the transaction is not tied to a specific
 * repository.
 */
open class TransactionManager(
    @PublishedApi internal val jdbi: Jdbi,
) {
  /**
   * Starts a database transaction, and runs the given [block] inside of it. Calls to [useHandle]
   * inside the block will use the same transaction (this includes the various methods on
   * [RepositoryJdbi]). If an exception is thrown, the transaction is rolled back.
   *
   * The repository's [Jdbi] instance is used for the transaction. If a transaction is already in
   * progress on the current thread, a new one will not be started (since we're already in a
   * transaction).
   *
   * ### Thread safety
   *
   * This function stores a transaction handle in a thread-local, so that operations within [block]
   * can get the handle. But new threads spawned in the scope of `block` will not see this
   * thread-local, and so they will not work correctly with the transaction. So you should not
   * attempt concurrent database operations with this function.
   *
   * ### Mocking
   *
   * See [shouldMockTransactions].
   */
  inline fun <ReturnT> transactional(block: () -> ReturnT): ReturnT {
    if (shouldMockTransactions()) {
      return block()
    }

    return transactional(jdbi, block)
  }

  /**
   * Since [transactional] is an inline method, it cannot be mocked by libraries such as
   * [mockk](https://mockk.io/) that operate on bytecode (since inline functions don't generate
   * bytecode). We still want `transactional` to be inline, to allow non-local returns in the lambda
   * passed to it, which is handy for such scope-based functions.
   *
   * So to support users who want to mock calls to `transactional`, we provide this method, which
   * users can override/mock to return true. When this returns true, `transactional` will just
   * immediately call the lambda passed to it, without starting a database transaction.
   *
   * ### Example
   *
   * ```
   * val mockTransactionManager =
   *     mockk<TransactionManager> { every { shouldMockTransactions() } returns true }
   *
   * val mockRepo =
   *     mockk<ExampleRepository> {
   *       every { getOrThrow(id = any(), forUpdate = any()) } returns mockEntity
   *       every { update(entity = any(), previousVersion = any()) } returns mockEntity
   *     }
   *
   * mockTransactionManager.transactional {
   *   val entity = mockRepo.getOrThrow(mockEntity.item.id, forUpdate = true)
   *   mockRepo.update(entity.item, entity.version)
   * }
   * ```
   */
  open fun shouldMockTransactions(): Boolean = false
}
