package no.liflig.documentstore.utils

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.result.BatchResultBearing
import org.jdbi.v3.core.statement.PreparedBatch

/**
 * Uses [Prepared Batches from JDBI](https://jdbi.org/releases/3.45.1/#_prepared_batches) to execute
 * the given [statement] on the given [items]. For each item, [bindParameters] is called to bind
 * parameters to the statement.
 *
 * The items are divided into multiple batches if the number of items exceeds [batchSize]. According
 * to Oracle,
 * [the optimal size for batch operations in JDBC is 50-100](https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754).
 * We default to the conservative end of 50, since we send JSON which is rather memory inefficient.
 *
 * [PreparedBatch.execute] returns an array of modified row counts (1 count for every batch item).
 * If you want to handle this, use [handleModifiedRowCounts]. This function is called once for every
 * executed batch, which may be more than 1 if the number of items exceeds [batchSize]. A second
 * parameter is provided to [handleModifiedRowCounts] with the start index of the current batch,
 * which can then be used to get the corresponding entity for diagnostics purposes.
 *
 * If you need to return something from the query, pass columns names in [columnsToReturn]. This
 * will append `RETURNING` to the SQL statement with the given column names. You can then iterate
 * over the results with [handleReturnedColumns].
 */
internal fun <BatchItemT> executeBatchOperation(
    handle: Handle,
    items: Iterable<BatchItemT>,
    statement: String,
    bindParameters: (PreparedBatch, BatchItemT) -> PreparedBatch,
    handleModifiedRowCounts: ((IntArray, Int) -> Unit)? = null,
    columnsToReturn: Array<String>? = null,
    handleReturnedColumns: ((BatchResultBearing) -> Unit)? = null,
    batchSize: Int = 50,
) {
  runWithAutoCommitDisabled(handle) {
    var currentBatch: PreparedBatch? = null
    var elementCountInCurrentBatch = 0
    var startIndexOfCurrentBatch = 0

    for ((index, element) in items.withIndex()) {
      if (currentBatch == null) {
        currentBatch = handle.prepareBatch(statement)!! // Should never return null
        startIndexOfCurrentBatch = index
      }

      currentBatch = bindParameters(currentBatch, element)
      currentBatch.add()
      elementCountInCurrentBatch++

      if (elementCountInCurrentBatch >= batchSize) {
        executeBatch(
            currentBatch,
            startIndexOfCurrentBatch,
            handleModifiedRowCounts,
            columnsToReturn,
            handleReturnedColumns,
        )

        currentBatch = null
        elementCountInCurrentBatch = 0
      }
    }

    // If currentBatch is non-null here, that means we still have remaining entities to update
    if (currentBatch != null) {
      executeBatch(
          currentBatch,
          startIndexOfCurrentBatch,
          handleModifiedRowCounts,
          columnsToReturn,
          handleReturnedColumns,
      )
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
private fun executeBatch(
    currentBatch: PreparedBatch,
    startIndexOfCurrentBatch: Int,
    handleModifiedRowCounts: ((IntArray, Int) -> Unit)?,
    columnsToReturn: Array<String>?,
    handleReturnedColumns: ((BatchResultBearing) -> Unit)?,
) {
  if (columnsToReturn.isNullOrEmpty()) {
    val modifiedRowCounts = currentBatch.execute()
    if (handleModifiedRowCounts != null) {
      handleModifiedRowCounts(modifiedRowCounts, startIndexOfCurrentBatch)
    }
  } else {
    val result = currentBatch.executePreparedBatch(*columnsToReturn)
    if (handleModifiedRowCounts != null) {
      handleModifiedRowCounts(result.modifiedRowCounts(), startIndexOfCurrentBatch)
    }
    if (handleReturnedColumns != null) {
      handleReturnedColumns(result)
    }
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
