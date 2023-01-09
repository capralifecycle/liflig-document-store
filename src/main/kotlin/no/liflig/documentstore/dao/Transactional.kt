package no.liflig.documentstore.dao

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

internal class CoroutineTransaction(
  val handle: Handle,
) : AbstractCoroutineContextElement(CoroutineTransaction) {
  companion object Key : CoroutineContext.Key<CoroutineTransaction>

  override fun toString(): String = "CoroutineTransaction(handle=$handle)"
}

@Deprecated("Use transactional with jdbi and explicit handle instead")
suspend fun <T> transactional(dao: CrudDao<*, *>, block: suspend () -> T): T = when (dao) {
  is CrudDaoJdbi -> {
    transactional(dao.jdbi) {
      block()
    }
  }

  else -> throw Error("Transactional requires JDBIDao")
}

suspend fun <T> transactional(jdbi: Jdbi, block: suspend (Handle) -> T): T =
  mapExceptions {
    jdbi.open().use { handle ->
      withContext(Dispatchers.IO + CoroutineTransaction(handle)) {
        try {
          jdbi.transactionHandler.begin(handle)
          block(handle)
            .also { jdbi.transactionHandler.commit(handle) }
        } catch (t: Throwable) {
          jdbi.transactionHandler.rollback(handle)
          throw t
        }
      }
    }
  }
