package no.liflig.documentstore.repository

import java.io.InterruptedIOException
import java.sql.SQLTransientException
import org.jdbi.v3.core.CloseException
import org.jdbi.v3.core.ConnectionException

/** An exception for an operation on a [Repository]. */
sealed class RepositoryException : RuntimeException()

/**
 * [RepositoryJdbi.update] and [RepositoryJdbi.delete] use optimistic locking: they require the
 * previous version of an entity to be passed in, and if the current version of the entity does not
 * match when trying to update/delete it, this exception is thrown. It likely means that the entity
 * was concurrently modified by another thread/server instance.
 */
class ConflictRepositoryException(override val message: String) : RepositoryException()

/** Exception thrown when a database query failed due to connection/network issues. */
class UnavailableRepositoryException(override val cause: Exception) : RepositoryException()

/** Exception thrown by [Repository.getOrThrow] when an entity was not found in the database. */
class EntityNotFoundException(override val message: String) : RepositoryException()

// `@PublishedApi` lets us use this in inline functions. Renaming or removing this may be a breaking
// change.
@PublishedApi
internal fun mapDatabaseException(e: Exception): Exception {
  return when (e) {
    is SQLTransientException,
    is InterruptedIOException,
    is ConnectionException,
    is CloseException -> UnavailableRepositoryException(cause = e)
    else -> e
  }
}

/**
 * Overload of [mapDatabaseException] for `Throwable`. We use this in some places where we catch
 * `Throwable`, but in other places we still want the `Exception`-only variant, so we keep this
 * separate.
 */
@PublishedApi
internal fun mapDatabaseException(e: Throwable): Throwable {
  return when (e) {
    is Exception -> mapDatabaseException(e)
    else -> e
  }
}
