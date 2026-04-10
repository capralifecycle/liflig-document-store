// We use Kotlin Contracts in `transactional`, for ergonomic use with lambdas. Contracts are an
// experimental feature, but they guarantee binary compatibility, so we can safely use them here.
@file:OptIn(ExperimentalContracts::class)

package no.liflig.documentstore.repository

import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import no.liflig.documentstore.DocumentStorePlugin
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.HandleScope
import org.jdbi.v3.core.Jdbi

/**
 * All [Jdbi] instances have a [HandleScope], which holds database [Handle]s in a thread-local scope
 * (such as for transactions from [Jdbi.inTransaction]). We want to use this for our [transactional]
 * and [useHandle] functions, so that they can interoperate with [Jdbi.inTransaction] and
 * [Jdbi.useHandle] (Document Store provides these alternate versions of the Jdbi methods, so that
 * we can make them `inline`, which is more ergonomic for scope functions in Kotlin).
 *
 * JDBI gives access to the handle scope with [Jdbi.getHandleScope], but its docstring explains that
 * it's an internal API, so we can't rely on it. But [Jdbi.setHandleScope] is public and intended
 * for external use, so we can use that in [DocumentStorePlugin] to override JDBI's [HandleScope]
 * with this global variable. By making it global, we get access to it in [transactional] and
 * [useHandle] without having to go through the internal [Jdbi.getHandleScope].
 */
internal val THREAD_LOCAL_TRANSACTION_HANDLE: HandleScope = HandleScope.threadLocal()

/**
 * Gets a database handle, either from an ongoing transaction (if this is called in a
 * [transactional] scope), or if none is found, gets a new handle with [Jdbi.open] (automatically
 * closed after the given [block] returns).
 *
 * This function is semantically equivalent to [Jdbi.useHandle] / [Jdbi.withHandle]. Reasons you may
 * want to use this instead:
 * - It's `inline`, which can be more ergonomic to use in Kotlin for scope functions such as this
 * - You don't have to provide a generic parameter for the exception type, which JDBI requires
 *   because of checked exceptions in Java
 */
inline fun <ReturnT> useHandle(jdbi: Jdbi, block: (Handle) -> ReturnT): ReturnT {
  val activeTransaction = getActiveTransactionHandle()
  if (activeTransaction != null) {
    return block(activeTransaction)
  }

  return openHandle(jdbi).use(block)
}

/**
 * Starts a database transaction, and runs the given [block] inside of it. Calls to [useHandle]
 * inside the block will use the same transaction (this includes the various methods on
 * [RepositoryJdbi]). If an exception is thrown, the transaction is rolled back.
 *
 * If a transaction is already in progress on the current thread, a new one will not be started
 * (since we're already in a transaction).
 *
 * This function is semantically equivalent to [Jdbi.inTransaction]. Reasons you may want to use
 * this instead:
 * - It's `inline`, which can be more ergonomic to use in Kotlin for scope functions such as this
 * - You don't have to provide a generic parameter for the exception type, which JDBI requires
 *   because of checked exceptions in Java
 */
inline fun <ReturnT> transactional(jdbi: Jdbi, block: () -> ReturnT): ReturnT {
  // Allows callers to use `block` as if it were in-place
  contract { callsInPlace(block, InvocationKind.EXACTLY_ONCE) }

  val activeTransaction = getActiveTransactionHandle()
  if (activeTransaction != null) {
    // This means we're already in a transaction, so we do not start a new one
    return block()
  }

  openHandle(jdbi).use { handle ->
    var shouldCommit = true
    try {
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
      beginTransaction(handle)
      return block()
    } catch (e: Throwable) {
      shouldCommit = false
      throw rollbackTransactionAndMapException(handle, e)
    } finally {
      // If `shouldCommit` is still true, that means we haven't rolled back, which means `block`
      // returned successfully, so we should commit.
      // See comment on `beginTransaction` above for why we call this here.
      endTransaction(handle, shouldCommit)
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
    // Allows callers to use `block` as if it were in-place
    contract { callsInPlace(block, InvocationKind.EXACTLY_ONCE) }

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
   *       every { getOrThrow(exampleId, forUpdate = any()) } returns mockEntity
   *       every { update(entity = any(), previousVersion = any()) } returns mockEntity
   *     }
   *
   * mockTransactionManager.transactional {
   *   val entity = mockRepo.getOrThrow(exampleId, forUpdate = true)
   *   mockRepo.update(entity.data, entity.version)
   * }
   * ```
   */
  open fun shouldMockTransactions(): Boolean = false
}

/**
 * Used by [transactional] and [useHandle]. We separate as much code as possible out from
 * [transactional] / [useHandle] into utility functions, because these functions are `inline`, and
 * having too much code inlined can bloat the compiled bytecode which in turn can lead to worse
 * performance.
 *
 * Since this is used in `inline` functions, renaming/removing/changing this function's signature
 * may break consumers (hence the `@PublishedApi` annotation).
 */
@PublishedApi
internal fun getActiveTransactionHandle(): Handle? {
  return THREAD_LOCAL_TRANSACTION_HANDLE.get()?.handle
}

/**
 * Used by [transactional] and [useHandle] (see [getActiveTransactionHandle] for why we separate
 * this out into its own function).
 *
 * Since this is used in `inline` functions, renaming/removing/changing this function's signature
 * may break consumers (hence the `@PublishedApi` annotation).
 */
@PublishedApi
internal fun openHandle(jdbi: Jdbi): Handle {
  try {
    return jdbi.open()
  } catch (e: Exception) {
    throw mapDatabaseException(e)
  }
}

/**
 * Used by [transactional] (see [getActiveTransactionHandle] for why we separate this out into its
 * own function).
 *
 * Since this is used in `inline` functions, renaming/removing/changing this function's signature
 * may break consumers (hence the `@PublishedApi` annotation).
 */
@PublishedApi
internal fun beginTransaction(handle: Handle) {
  THREAD_LOCAL_TRANSACTION_HANDLE.set(handle)
  handle.begin()
}

/**
 * Used by [transactional] (see [getActiveTransactionHandle] for why we separate this out into its
 * own function).
 *
 * Since this is used in `inline` functions, renaming/removing/changing this function's signature
 * may break consumers (hence the `@PublishedApi` annotation).
 */
@PublishedApi
internal fun rollbackTransactionAndMapException(handle: Handle, exception: Throwable): Throwable {
  try {
    handle.rollback()
  } catch (rollback: Exception) {
    // If rollback failed, we still want to throw the original exception, so we add this
    // exception as a suppressed exception here. This is the same as JDBI does in their
    // implementation of `inTransaction`.
    exception.addSuppressed(rollback)
  }

  return mapDatabaseException(exception)
}

/**
 * Used by [transactional] (see [getActiveTransactionHandle] for why we separate this out into its
 * own function).
 *
 * Since this is used in `inline` functions, renaming/removing/changing this function's signature
 * may break consumers (hence the `@PublishedApi` annotation).
 */
@PublishedApi
internal fun endTransaction(handle: Handle, shouldCommit: Boolean) {
  THREAD_LOCAL_TRANSACTION_HANDLE.clear()

  if (shouldCommit) {
    handle.commit()
  }
}

/**
 * TODO: Remove this, as it's no longer used (replaced by [THREAD_LOCAL_TRANSACTION_HANDLE]).
 *
 * We didn't remove this immediately, as it was used in inline functions (hence [PublishedApi]), so
 * consumers may indirectly still depend on this through transitive dependencies. Should probably be
 * fine to remove in a couple months from the time of writing.
 */
@Deprecated("Scheduled for removal", level = DeprecationLevel.ERROR)
@Suppress("unused")
@PublishedApi
internal val transactionHandle = ThreadLocal<Handle?>()
