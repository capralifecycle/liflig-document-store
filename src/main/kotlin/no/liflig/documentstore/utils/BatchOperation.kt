package no.liflig.documentstore.utils

import java.sql.BatchUpdateException
import kotlin.math.min
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.Versioned
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.result.BatchResultBearing
import org.jdbi.v3.core.statement.PreparedBatch
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/**
 * Uses [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to execute
 * the given [statement] on the given [entities]. For each entity, [bindParameters] is called to
 * bind parameters to the statement.
 *
 * The entities are divided into multiple batches if the number of entities exceeds [batchSize].
 * According to Oracle,
 * [the optimal size for batch operations in JDBC is 50-100](https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754).
 * We default to the conservative end of 50, since we send JSON which is rather memory inefficient.
 *
 * [PreparedBatch.execute] returns an array of modified row counts (1 count for every entity). If
 * you want to handle this, use [handleModifiedRowCounts]. This function is called once for every
 * executed batch, which may be more than 1 if the number of entities exceeds [batchSize]. The first
 * parameter is an array of the modified row counts for the executed batch, and the second parameter
 * is the batch of entities.
 *
 * If you need to return something from the query, pass columns names in [columnsToReturn]. This
 * will append `RETURNING` to the SQL statement with the given column names. You can then iterate
 * over the results with [handleReturnedColumns].
 *
 * @throws BatchOperationException If the batch operation failed because of a single entity (e.g. a
 *   unique constraint violation).
 */
internal fun <EntityT> executeBatchOperation(
    handle: Handle,
    entities: Iterable<EntityT>,
    statement: String,
    bindParameters: (PreparedBatch, EntityT) -> PreparedBatch,
    handleModifiedRowCounts: ((IntArray, List<EntityT>) -> Unit)? = null,
    columnsToReturn: Array<String>? = null,
    handleReturnedColumns: ((BatchResultBearing) -> Unit)? = null,
    batchSize: Int = 50,
) {
  runWithAutoCommitDisabled(handle) {
    val batchProvider = BatchProvider.create(entities, batchSize)

    var batch = batchProvider.nextBatch()
    while (batch != null) {
      var batchStatement: PreparedBatch = handle.prepareBatch(statement)

      for (entity in batch) {
        batchStatement = bindParameters(batchStatement, entity)
        batchStatement.add()
      }

      try {
        executeBatch(
            batchStatement,
            batch,
            handleModifiedRowCounts,
            columnsToReturn,
            handleReturnedColumns,
        )
      } catch (e: UnableToExecuteStatementException) {
        val failingEntity = getFailingEntity(batch, e)
        if (failingEntity != null) {
          throw BatchOperationException(failingEntity, cause = e)
        } else {
          throw e
        }
      }

      batch = batchProvider.nextBatch()
    }
  }
}

/**
 * We have 2 different variants here:
 * - If the batch query is not returning anything, we can call [PreparedBatch.execute], which just
 *   returns the modified row counts.
 * - If the batch query does return something (i.e. [columnsToReturn] is set), then we must call
 *   [PreparedBatch.executePreparedBatch]. That appends the given columns in a `RETURNING` clause on
 *   the query, and gives us a result object which we can handle in [handleReturnedColumns].
 */
private fun <EntityT> executeBatch(
    batchStatement: PreparedBatch,
    batch: List<EntityT>,
    handleModifiedRowCounts: ((IntArray, List<EntityT>) -> Unit)?,
    columnsToReturn: Array<String>?,
    handleReturnedColumns: ((BatchResultBearing) -> Unit)?,
) {
  if (columnsToReturn.isNullOrEmpty()) {
    val modifiedRowCounts = batchStatement.execute()
    if (handleModifiedRowCounts != null) {
      handleModifiedRowCounts(modifiedRowCounts, batch)
    }
  } else {
    val result = batchStatement.executePreparedBatch(*columnsToReturn)
    if (handleModifiedRowCounts != null) {
      handleModifiedRowCounts(result.modifiedRowCounts(), batch)
    }
    if (handleReturnedColumns != null) {
      handleReturnedColumns(result)
    }
  }
}

/**
 * We want [executeBatchOperation] to take an [Iterable], so that it can be used both with large
 * streams of entities (like we do in [no.liflig.documentstore.migration.migrateEntity]), and
 * in-memory lists of entities.
 *
 * As we iterate over entities to add them to a batch, we also want to store a list of the current
 * batch of entities. We need this to get the failing entity for [BatchOperationException], which is
 * useful to know exactly which entity in the batch failed.
 *
 * If the `Iterable` represents a stream of entities that may not all be in memory, we must create
 * this entity list ourselves and add to it as we consume the stream. This is what
 * [StreamingBatchProvider] does. But if the `Iterable` is already an in-memory list, we want to
 * avoid the overhead of allocating these additional lists, and instead use [List.subList] to create
 * a view of the list without copying. This is what [InMemoryBatchProvider] does.
 */
private interface BatchProvider<EntityT> {
  /** Returns null when empty. */
  fun nextBatch(): List<EntityT>?

  companion object {
    fun <EntityT> create(entities: Iterable<EntityT>, batchSize: Int): BatchProvider<EntityT> {
      return if (entities is List) {
        InMemoryBatchProvider(entities, batchSize)
      } else {
        StreamingBatchProvider(entities.iterator(), batchSize)
      }
    }
  }
}

private class InMemoryBatchProvider<EntityT>(
    private val entities: List<EntityT>,
    private val batchSize: Int,
) : BatchProvider<EntityT> {
  private var startIndexOfCurrentBatch = 0

  override fun nextBatch(): List<EntityT>? {
    if (startIndexOfCurrentBatch >= entities.size) {
      return null
    }

    val endIndex = min(startIndexOfCurrentBatch + batchSize, entities.size)
    val batch = entities.subList(startIndexOfCurrentBatch, endIndex)
    startIndexOfCurrentBatch = endIndex
    return batch
  }
}

private class StreamingBatchProvider<BatchItemT>(
    private val entities: Iterator<BatchItemT>,
    private val batchSize: Int,
) : BatchProvider<BatchItemT> {
  // Initialize with capacity equal to batchSize
  private val currentBatch = ArrayList<BatchItemT>(batchSize)

  override fun nextBatch(): List<BatchItemT>? {
    currentBatch.clear()

    while (entities.hasNext() && currentBatch.size < batchSize) {
      currentBatch.add(entities.next())
    }

    return if (currentBatch.isEmpty()) null else currentBatch
  }
}

/**
 * We throw this exception if a batch operation failed due to a single entity, e.g. a unique
 * constraint violation.
 */
internal class BatchOperationException(
    val entity: Entity<*>,
    override val cause: Exception,
) : RuntimeException() {
  override val message
    get() = "Batch operation failed for entity: ${entity}"
}

private const val BATCH_ENTRY_EXCEPTION_PREFIX = "Batch entry"

/**
 * When [PreparedBatch.execute] throws, we would like to know if the failure was caused by a
 * specific entity in the batch, e.g. in case a constraint was violated.
 *
 * For some JDBC drivers, one can use [BatchUpdateException.updateCounts] for this. But the Postgres
 * driver [does not provide the info we need from it](https://github.com/pgjdbc/pgjdbc/issues/670).
 *
 * However, the Postgres driver does include the batch index of the entity that failed in the
 * [exception message when it throws BatchUpdateException](https://github.com/pgjdbc/pgjdbc/blob/cf3d8e5ed1bca96873735a8731ed4082132361a5/pgjdbc/src/main/java/org/postgresql/jdbc/BatchResultHandler.java#L162-L164).
 * So we can try to parse out the index from the exception here, and use that to get the failing
 * entity. This is a kind of hacky solution, but presumably the exception message format of the
 * Postgres driver is pretty stable. And if it changes, it will be caught by our tests.
 */
private fun <EntityT> getFailingEntity(
    batch: List<EntityT>,
    exception: UnableToExecuteStatementException
): Entity<*>? {
  try {
    val cause = exception.cause
    if (cause !is BatchUpdateException) {
      return null
    }

    val message = cause.message ?: return null
    if (!message.startsWith(BATCH_ENTRY_EXCEPTION_PREFIX)) {
      return null
    }

    // The exception message looks like "Batch entry X ...", where X is the index we want. So we
    // skip "Batch entry ", and look for the next space after that - the string between those two
    // should be our index string.
    val start = BATCH_ENTRY_EXCEPTION_PREFIX.length + 1
    val end = message.indexOf(' ', startIndex = start)
    val indexString = message.substring(start, end)
    val index = indexString.toInt()

    // executeBatchOperation is either called on a collection of Entity<*>, or Versioned<Entity<*>>.
    // We want to unpack to an Entity<*> here, so that BatchOperationException can use just the
    // Entity.
    return when (val entity = batch.getOrNull(index)) {
      is Entity<*> -> entity
      is Versioned<*> -> entity.item
      else -> null
    }
  } catch (e: Exception) {
    return null
  }
}

/**
 * When using batch operations, we typically want to send the batches as part of a transaction, so
 * that either the full operation is committed, or none at all. To do this, we have to make sure
 * that "auto-commit" is disabled on the database connection.
 * [This is what the JDBC docs recommend](https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#batch_updates).
 */
private inline fun runWithAutoCommitDisabled(handle: Handle, block: () -> Unit) {
  var autoCommitWasEnabled = false
  if (handle.connection.autoCommit) {
    handle.connection.autoCommit = false
    autoCommitWasEnabled = true
  }

  try {
    block()
  } finally {
    if (autoCommitWasEnabled) {
      try {
        handle.connection.autoCommit = true
      } catch (_: Exception) {}
    }
  }
}
