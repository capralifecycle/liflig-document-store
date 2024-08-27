package no.liflig.documentstore.dao

import java.io.InterruptedIOException
import java.sql.SQLTransientException
import org.jdbi.v3.core.CloseException
import org.jdbi.v3.core.ConnectionException

/** An exception for an operation on a DAO (Data Access Object). */
@Deprecated(
    "Replaced by RepositoryException.",
    ReplaceWith(
        "RepositoryException",
        imports = ["no.liflig.documentstore.repository.RepositoryException"],
    ),
    level = DeprecationLevel.WARNING,
)
sealed class DaoException : RuntimeException {
  // Use two constructors instead of a single constructor with nullable parameter to avoid nulling
  // out 'cause' further up the hierarchy (in [Throwable]) if no exception is to be passed
  constructor() : super()
  constructor(e: Exception) : super(e)
}

@Deprecated(
    "Replaced by ConflictRepositoryException.",
    ReplaceWith(
        "ConflictRepositoryException",
        imports = ["no.liflig.documentstore.repository.ConflictRepositoryException"],
    ),
    level = DeprecationLevel.WARNING,
)
class ConflictDaoException : DaoException()

@Deprecated(
    "Replaced by UnavailableRepositoryException.",
    ReplaceWith(
        "UnavailableRepositoryException",
        imports = ["no.liflig.documentstore.repository.UnavailableRepositoryException"],
    ),
    level = DeprecationLevel.WARNING,
)
data class UnavailableDaoException(
    val e: Exception,
) : DaoException(e)

@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store. If you use this with jdbi.open(), you should instead use 'useHandle' (no.liflig.documentstore.repository.useHandle).",
    level = DeprecationLevel.WARNING,
)
inline fun <T> mapExceptions(block: () -> T): T {
  try {
    return block()
  } catch (e: Exception) {
    when (e) {
      is ConflictDaoException -> throw e
      is SQLTransientException,
      is InterruptedIOException,
      is ConnectionException,
      is CloseException -> throw UnavailableDaoException(e)
      else -> throw e
    }
  }
}
