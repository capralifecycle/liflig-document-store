@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.migration

import java.text.DecimalFormat
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.repository.RepositoryJdbi
import no.liflig.documentstore.repository.useHandle
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleId
import no.liflig.documentstore.testutils.InstantSerializer
import no.liflig.documentstore.testutils.KotlinSerialization
import no.liflig.documentstore.testutils.MIGRATION_TABLE
import no.liflig.documentstore.testutils.MigratedExampleEntity
import no.liflig.documentstore.testutils.dataSource
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoPostMigration
import no.liflig.documentstore.testutils.exampleRepoPreMigration
import no.liflig.documentstore.testutils.jdbi
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class MigrationTest {
  @AfterEach
  fun clear() {
    useHandle(jdbi) { handle -> handle.createUpdate("TRUNCATE ${MIGRATION_TABLE}").execute() }
  }

  // For deterministic sorting
  private val numberFormat = DecimalFormat("00000")

  @Test
  fun `test migration`() {
    val existingEntities =
        (0 until 10_000)
            .asSequence()
            .map { number -> ExampleEntity(text = numberFormat.format(number)) }
            .asIterable()
    exampleRepoPreMigration.batchCreate(existingEntities)

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_1__Test_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = MIGRATION_TABLE,
            serializationAdapter = KotlinSerialization(MigratedExampleEntity.serializer()),
            transformEntity = { (entity, _) ->
              entity.copy(newFieldAfterMigration = "new-field-${entity.text}")
            },
        )
      }
    }

    runMigration(V001_1__Test_migration())

    var count = 0
    exampleRepoPostMigration.streamAll { entities ->
      for ((index, entity) in entities.withIndex()) {
        assertEquals(
            "new-field-${numberFormat.format(index)}",
            entity.item.newFieldAfterMigration,
        )
        count++
      }
    }

    assertEquals(10_000, count)
  }

  @Test
  fun `migration rolls back on failed transaction`() {
    val entitiesToCreate =
        (0 until 10).map { number ->
          ExampleEntity(text = "Original", optionalText = number.toString())
        }
    exampleRepoPreMigration.batchCreate(entitiesToCreate)
    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_2__Failed_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = MIGRATION_TABLE,
            serializationAdapter = KotlinSerialization(MigratedExampleEntity.serializer()),
            transformEntity = { (entity, _) ->
              // Throw halfway through transaction
              if (entity.optionalText == "5") {
                throw Exception("Rolling back transaction")
              }
              entity.copy(text = "Migrated")
            },
        )
      }
    }

    assertFailsWith<Exception> { runMigration(V001_2__Failed_migration()) }

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.item.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
    for (entity in fetchedEntities) {
      assertEquals("Original", entity.item.text)
    }
  }

  @Test
  fun `migration with WHERE clause only migrates matching entities`() {
    val entitiesToMigrate = (0 until 50).map { ExampleEntity(text = "SHOULD_MIGRATE") }
    val entitiesNotToMigrate = (0 until 50).map { ExampleEntity(text = "SHOULD_NOT_MIGRATE") }
    exampleRepoPreMigration.batchCreate(entitiesToMigrate + entitiesNotToMigrate)

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_3__Migration_with_WHERE_clause : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = MIGRATION_TABLE,
            serializationAdapter = KotlinSerialization(ExampleEntity.serializer()),
            transformEntity = { (entity, _) -> entity.copy(text = "MIGRATED") },
            where = "data->>'text' = :text",
            bindParameters = { bind("text", "SHOULD_MIGRATE") },
        )
      }
    }

    runMigration(V001_3__Migration_with_WHERE_clause())

    // We use exampleRepoPreMigration, since we don't want to add fields in this case, just change
    // existing ones
    val migratedEntities = exampleRepoPreMigration.listByIds(entitiesToMigrate.map { it.id })
    assertEquals(entitiesToMigrate.size, migratedEntities.size)
    migratedEntities.forEach { entity -> assertEquals("MIGRATED", entity.item.text) }

    val notMigratedEntities = exampleRepoPreMigration.listByIds(entitiesNotToMigrate.map { it.id })
    assertEquals(entitiesNotToMigrate.size, notMigratedEntities.size)
    notMigratedEntities.forEach { entity -> assertEquals("SHOULD_NOT_MIGRATE", entity.item.text) }
  }

  // Disabled for automatic tests, since this is a heavy test that we don't want to run in CI
  @Disabled
  @Test
  fun `load-test migration`() {
    val existingEntities =
        (0L until 500_000L)
            .asSequence()
            .map { number ->
              // Create unique values for each field, to make the data more realistic
              LargeEntityPreMigration(
                  field1 = TwoStrings("field1-${number}", "field1-${number + 1}"),
                  field2 = StringAndInstant("field2-${number}", Instant.now()),
                  field3 =
                      EnumAndInstant(ExampleEnum.valueOf("VALUE_${number % 4}"), Instant.now()),
                  field4 = listOf("field4-${number}", "field4-${number + 1}"),
                  field5 = number,
                  field6 = number % 2 == 0L,
                  field7 = "field7-${number}",
                  field8 = "field8-${number}",
              )
            }
            .asIterable()
    largeEntityRepoPreMigration.batchCreate(existingEntities)

    // Give some time after batchCreate, to more accurately simulate migration after creation
    Thread.sleep(1000)

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_4__Load_test_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = MIGRATION_TABLE,
            serializationAdapter = KotlinSerialization(LargeEntityPostMigration.serializer()),
        )
      }
    }

    runMigration(V001_4__Load_test_migration())
  }

  private fun runMigration(migration: BaseJavaMigration) {
    Flyway.configure()
        .baselineOnMigrate(true)
        .baselineDescription("firstInit")
        .dataSource(dataSource)
        .locations("migrations")
        .javaMigrations(migration)
        // Ignore missing migrations, since we run different migrations in different tests
        .ignoreMigrationPatterns("versioned:missing")
        .load()
        .migrate()
  }
}

private val largeEntityRepoPreMigration: LargeEntityRepoPreMigration by lazy {
  LargeEntityRepoPreMigration(jdbi)
}

private class LargeEntityRepoPreMigration(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, LargeEntityPreMigration>(
        jdbi,
        // We can re-use the migration table here, since we truncate it between every test
        tableName = MIGRATION_TABLE,
        serializationAdapter = KotlinSerialization(LargeEntityPreMigration.serializer()),
    )

/**
 * An example of a larger entity than [ExampleEntity], for load-testing the repository migration API
 * with more realistic data.
 */
@Serializable
private data class LargeEntityPreMigration(
    override val id: ExampleId = ExampleId(),
    val field1: TwoStrings,
    val field2: StringAndInstant,
    val field3: EnumAndInstant,
    val field4: List<String>,
    val field5: Long,
    val field6: Boolean,
    val field7: String,
    val field8: String,
) : Entity<ExampleId>

@Serializable
private data class LargeEntityPostMigration(
    override val id: ExampleId = ExampleId(),
    val field1: TwoStrings,
    val field2: StringAndInstant,
    val field3: EnumAndInstant,
    val field4: List<String>,
    val field5: Long,
    val field6: Boolean,
    val field7: String,
    val field8: String,
    val field9: String? = null,
    val field10: String = field7,
) : Entity<ExampleId>

@Serializable
private data class TwoStrings(
    val field1: String,
    val field2: String,
)

@Serializable
private data class StringAndInstant(
    val field1: String,
    val field2: Instant,
)

@Serializable
private data class EnumAndInstant(
    val field1: ExampleEnum,
    val field2: Instant,
)

@Serializable
enum class ExampleEnum {
  VALUE_0,
  VALUE_1,
  VALUE_2,
  VALUE_3,
}
