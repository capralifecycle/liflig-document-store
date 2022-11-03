package no.liflig.documentstore.dao

import org.jdbi.v3.core.Handle
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

internal class CoroutineTransaction(
  val handle: Handle,
) : AbstractCoroutineContextElement(CoroutineTransaction) {
  companion object Key : CoroutineContext.Key<CoroutineTransaction>

  override fun toString(): String = "CoroutineTransaction(handle=$handle)"
}

@Deprecated("Use TransactionFactory and explicit handle instead")
suspend fun <T> transactional(dao: CrudDao<*, *>, block: suspend () -> T): T = when (dao) {
  is CrudDaoJdbi -> {
    TransactionFactory(dao.jdbi).transactional {
      block()
    }
  }

  else -> throw Error("Transactional requires JDBIDao")
}
