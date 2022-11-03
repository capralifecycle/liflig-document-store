package no.liflig.documentstore.dao

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi

class TransactionFactory(private val jdbi: Jdbi) {
  suspend fun <T> transactional(block: suspend (Handle) -> T): T =
    mapExceptions {
      jdbi.open().use { handle ->
        withContext(Dispatchers.IO + CoroutineTransaction(handle)) {
          try {
            jdbi.transactionHandler.begin(handle)
            block(handle)
              .also { jdbi.transactionHandler.commit(handle) }
          } finally {
            if (jdbi.transactionHandler.isInTransaction(handle))
              jdbi.transactionHandler.rollback(handle)
          }
        }
      }
    }
}
