package no.liflig.documentstore.dao

import java.time.Instant
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.EntityRoot
import no.liflig.documentstore.entity.Version
import no.liflig.documentstore.entity.VersionedEntity
import org.jdbi.v3.core.Jdbi

/**
 * A DAO (Data Access Object) for CRUD (Create, Read, Update, Delete) operations on entities in a
 * database table.
 */
interface CrudDao<I : EntityId, A : EntityRoot<I>> {
  fun create(entity: A): VersionedEntity<A>

  fun get(id: I, forUpdate: Boolean = false): VersionedEntity<A>?

  fun <A2 : A> update(
      entity: A2,
      previousVersion: Version,
  ): VersionedEntity<A2>

  fun delete(id: I, previousVersion: Version)
}

/** An implementation of [CrudDao] that uses the JDBI library for database access. */
class CrudDaoJdbi<I : EntityId, A : EntityRoot<I>>(
    private val jdbi: Jdbi,
    private val sqlTableName: String,
    private val serializationAdapter: SerializationAdapter<A>,
) : CrudDao<I, A> {
  private fun toJson(entity: A): String = serializationAdapter.toJson(entity)
  private fun fromJson(value: String): A = serializationAdapter.fromJson(value)
  private val rowMapper = createRowMapper(createRowParser(::fromJson))

  override fun get(id: I, forUpdate: Boolean): VersionedEntity<A>? =
      getHandle(jdbi) { handle ->
        handle
            .select(
                """
                  SELECT id, data, version, created_at, modified_at
                  FROM "$sqlTableName"
                  WHERE id = :id
                  ORDER BY created_at
                  ${if (forUpdate) " FOR UPDATE" else ""}
                """
                    .trimIndent(),
            )
            .bind("id", id)
            .map(rowMapper)
            .firstOrNull()
      }

  override fun delete(id: I, previousVersion: Version) =
      getHandle(jdbi) { handle ->
        val deleted =
            handle
                .createUpdate(
                    """
                      DELETE FROM "$sqlTableName"
                      WHERE id = :id AND version = :previousVersion
                    """
                        .trimIndent(),
                )
                .bind("id", id)
                .bind("previousVersion", previousVersion)
                .execute()

        if (deleted == 0) throw ConflictDaoException() else Unit
      }

  /**
   * Default implementation for create. Note that some repositories might need to implement its own
   * version if there are special columns that needs to be kept in sync e.g. for indexing purposes.
   */
  override fun create(entity: A): VersionedEntity<A> =
      getHandle(jdbi) { handle ->
        VersionedEntity(entity, Version.initial()).also {
          val now = Instant.now()
          handle
              .createUpdate(
                  """
                    INSERT INTO "$sqlTableName" (id, version, data, modified_at, created_at)
                    VALUES (:id, :version, :data::jsonb, :modifiedAt, :createdAt)
                  """
                      .trimIndent(),
              )
              .bind("id", entity.id)
              .bind("version", it.version)
              .bind("data", toJson(entity))
              .bind("modifiedAt", now)
              .bind("createdAt", now)
              .execute()
        }
      }

  /**
   * Default implementation for update. Note that some repositories might need to implement its own
   * version if there are special columns that needs to be kept in sync e.g. for indexing purposes.
   */
  override fun <A2 : A> update(entity: A2, previousVersion: Version): VersionedEntity<A2> =
      getHandle(jdbi) { handle ->
        val result = VersionedEntity(entity, previousVersion.next())
        val updated =
            handle
                .createUpdate(
                    """
                      UPDATE "$sqlTableName"
                      SET
                        version = :nextVersion,
                        data = :data::jsonb,
                        modified_at = :modifiedAt
                      WHERE
                        id = :id AND
                        version = :previousVersion
                    """
                        .trimIndent(),
                )
                .bind("nextVersion", result.version)
                .bind("data", toJson(entity))
                .bind("id", entity.id)
                .bind("modifiedAt", Instant.now())
                .bind("previousVersion", previousVersion)
                .execute()

        if (updated == 0) throw ConflictDaoException() else result
      }
}
