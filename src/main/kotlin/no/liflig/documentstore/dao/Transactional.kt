package no.liflig.documentstore.dao

import kotlinx.coroutines.withContext
import no.liflig.documentstore.CoroutineJdbiWrapper
import org.jdbi.v3.core.Handle
import java.lang.Exception
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

internal class CoroutineTransaction(
  val handle: Handle,
) : AbstractCoroutineContextElement(CoroutineTransaction) {
  companion object Key : CoroutineContext.Key<CoroutineTransaction>

  override fun toString(): String = "CoroutineTransaction(handle=$handle)"
}

suspend fun <T> transactional(limiter: CoroutineJdbiWrapper, block: suspend (Handle) -> T): T {
  val existingHandle = coroutineContext[CoroutineTransaction]?.handle

  // We can assume that we're already in a transaction, so we just pass the existing handle into the block
  return if (existingHandle != null) {
    block(existingHandle)
  } else
    mapExceptions {
      limiter.withHandle { handle ->
        withContext(CoroutineTransaction(handle)) {
          handle.begin()
          try {
            val result = block(handle)
            handle.commit()
            result
          } catch (e: Exception) {
            handle.rollback()
            throw e
          }
        }
      }
    }
}
