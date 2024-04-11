package no.liflig.documentstore.dao

import java.io.InterruptedIOException
import java.sql.SQLTransientException
import org.jdbi.v3.core.CloseException
import org.jdbi.v3.core.ConnectionException

/** An exception for an operation on a DAO (Data Access Object). */
sealed class DaoException : RuntimeException {
  // Use two constructors instead of a single constructor with nullable parameter to avoid nulling
  // out 'cause' further up the hierarchy (in [Throwable]) if no exception is to be passed
  constructor() : super()
  constructor(e: Exception) : super(e)
}

class ConflictDaoException : DaoException()

data class UnavailableDaoException(
    val e: Exception,
) : DaoException(e)

data class UnknownDaoException(
    val e: Exception,
) : DaoException(e)

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
      else -> throw UnknownDaoException(e)
    }
  }
}
