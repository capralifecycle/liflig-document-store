package no.liflig.documentstore.dao

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Handle
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext


@PublishedApi
internal class CoroutineTransaction(
  val handle: Handle,
) : AbstractCoroutineContextElement(CoroutineTransaction) {
  companion object Key : CoroutineContext.Key<CoroutineTransaction>
  override fun toString(): String = "CoroutineTransaction(handle=$handle)"
}

suspend fun transactional(dao: CrudDao<*, *>, block: suspend () -> Unit) {

  return when (dao) {
    is CrudDaoJdbi -> mapExceptions {
      dao.jdbi.open().useTransaction<Exception> { handle ->
        runBlocking {
          withContext(Dispatchers.IO + CoroutineTransaction(handle)) {
            block()
          }
        }
      }
    }

    else -> throw Error("Transactional requires JDBIDao")
  }
}
