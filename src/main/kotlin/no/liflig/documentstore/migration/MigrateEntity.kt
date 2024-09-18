package no.liflig.documentstore.migration

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.StringEntityId
import no.liflig.documentstore.entity.UuidEntityId
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.EntityRowMapper
import no.liflig.documentstore.repository.SerializationAdapter

fun <EntityT : Entity<*>> migrateEntity(
    dbConnection: Connection,
    tableName: String,
    serializationAdapter: SerializationAdapter<EntityT>,
    transformEntity: ((Versioned<EntityT>) -> EntityT)? = null,
) {
  runWithAutoCommitDisabled(dbConnection) {
    streamAllRows(dbConnection, tableName) { resultSet ->
      batchUpdate(dbConnection, tableName, resultSet, serializationAdapter, transformEntity)
    }
  }
}

private inline fun streamAllRows(
    dbConnection: Connection,
    tableName: String,
    consumer: (ResultSet) -> Unit
) {
  dbConnection.createStatement().use { statement ->
    statement.fetchSize = MIGRATION_BATCH_SIZE
    statement
        .executeQuery(
            // language=string
            """
              SELECT id, data, version, created_at, modified_at
              FROM "${tableName}"
              FOR UPDATE
            """
                .trimIndent(),
        )
        .use(consumer)
  }
}

private fun <EntityT : Entity<*>> batchUpdate(
    dbConnection: Connection,
    tableName: String,
    resultSet: ResultSet,
    serializationAdapter: SerializationAdapter<EntityT>,
    transformEntity: ((Versioned<EntityT>) -> EntityT)?
) {
  val rowMapper = EntityRowMapper(serializationAdapter)

  while (resultSet.next()) {
    dbConnection
        .prepareStatement(
            // language=string
            """
              UPDATE "${tableName}"
              SET
                data = ?,
                version = ?,
                modified_at = ?
              WHERE
                id = ?
            """
                .trimIndent(),
        )
        .use { statement ->
          var elementCountInCurrentBatch = 0
          do {
            elementCountInCurrentBatch++

            val entity = rowMapper.mapEntity(resultSet)
            val updatedEntity =
                if (transformEntity == null) entity.item else transformEntity(entity)

            // Parameter indices start at 1
            statement.setString(1, serializationAdapter.toJson(updatedEntity))
            statement.setLong(2, entity.version.value)
            statement.setObject(3, Instant.now(), Types.TIMESTAMP_WITH_TIMEZONE)
            when (val id = entity.item.id) {
              is StringEntityId -> statement.setString(4, id.value)
              is UuidEntityId -> statement.setObject(4, id.value)
            }

            statement.addBatch()
          } while (resultSet.next() && elementCountInCurrentBatch <= MIGRATION_BATCH_SIZE)

          statement.executeBatch()
        }
  }
}

private inline fun runWithAutoCommitDisabled(dbConnection: Connection, block: () -> Unit) {
  var autoCommitWasEnabled = false
  if (dbConnection.autoCommit) {
    dbConnection.autoCommit = false
    autoCommitWasEnabled = true
  }

  try {
    block()
  } finally {
    if (autoCommitWasEnabled) {
      try {
        dbConnection.autoCommit = true
      } catch (_: Exception) {}
    }
  }
}

/**
 * According to Oracle, the optimal size for batch database operations with JDBC is 50-100:
 * https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
 *
 * We stay on the higher end of 100 for migrations, since migrations are called on startup, so
 * presumably there is less memory contention.
 */
internal const val MIGRATION_BATCH_SIZE = 100
