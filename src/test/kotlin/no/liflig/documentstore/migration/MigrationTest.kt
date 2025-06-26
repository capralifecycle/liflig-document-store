@file:UseSerializers(InstantSerializer::class)

package no.liflig.documentstore.migration

import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldNotBeEmpty
import java.text.DecimalFormat
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.repository.RepositoryJdbi
import no.liflig.documentstore.testutils.ExampleEntity
import no.liflig.documentstore.testutils.ExampleId
import no.liflig.documentstore.testutils.InstantSerializer
import no.liflig.documentstore.testutils.KotlinSerialization
import no.liflig.documentstore.testutils.MigratedExampleEntity
import no.liflig.documentstore.testutils.clearDatabase
import no.liflig.documentstore.testutils.dataSource
import no.liflig.documentstore.testutils.exampleRepo
import no.liflig.documentstore.testutils.exampleRepoPostMigration
import no.liflig.documentstore.testutils.jdbi
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class MigrationTest {
  @BeforeEach
  fun reset() {
    clearDatabase()
  }

  // For deterministic sorting
  private val numberFormat = DecimalFormat("00000")

  @Test
  @Order(1)
  fun `test migration`() {
    val existingEntities =
        (0 until 10_000)
            .asSequence()
            .map { number -> ExampleEntity(text = numberFormat.format(number)) }
            .asIterable()
    exampleRepo.batchCreate(existingEntities)

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_1__Test_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = "example",
            serializationAdapter = KotlinSerialization(MigratedExampleEntity.serializer()),
            transform = { (entity, _) ->
              entity.copy(newFieldAfterMigration = "new-field-${entity.text}")
            },
        )
      }
    }

    runMigration(V001_1__Test_migration())

    var count = 0
    exampleRepoPostMigration.streamAll { entities ->
      for (entity in entities) {
        assertEquals(
            "new-field-${numberFormat.format(count)}",
            entity.data.newFieldAfterMigration,
        )
        count++
      }
    }

    assertEquals(10_000, count)
  }

  @Test
  @Order(2)
  fun `migration rolls back on failed transaction`() {
    val entitiesToCreate =
        (0 until 10).map { number ->
          ExampleEntity(text = "Original", optionalText = number.toString())
        }
    exampleRepo.batchCreate(entitiesToCreate)
    val createdEntities = exampleRepo.listByIds(entitiesToCreate.map { it.id })

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_2__Failed_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = "example",
            serializationAdapter = KotlinSerialization(MigratedExampleEntity.serializer()),
            transform = { (entity, _) ->
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

    val fetchedEntities = exampleRepo.listByIds(createdEntities.map { it.data.id })
    assertEquals(createdEntities.size, fetchedEntities.size)
    for (entity in fetchedEntities) {
      assertEquals("Original", entity.data.text)
    }
  }

  @Test
  @Order(3)
  fun `migration with WHERE clause only migrates matching entities`() {
    val entitiesToMigrate = (0 until 50).map { ExampleEntity(text = "SHOULD_MIGRATE") }
    val entitiesNotToMigrate = (0 until 50).map { ExampleEntity(text = "SHOULD_NOT_MIGRATE") }
    exampleRepo.batchCreate(entitiesToMigrate + entitiesNotToMigrate)

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_3__Migration_with_WHERE_clause : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = "example",
            serializationAdapter = KotlinSerialization(ExampleEntity.serializer()),
            transform = { (entity, _) -> entity.copy(text = "MIGRATED") },
            where = "data->>'text' = :text",
            bindParameters = { bind("text", "SHOULD_MIGRATE") },
        )
      }
    }

    runMigration(V001_3__Migration_with_WHERE_clause())

    // We use exampleRepoPreMigration, since we don't want to add fields in this case, just change
    // existing ones
    val migratedEntities = exampleRepo.listByIds(entitiesToMigrate.map { it.id })
    assertEquals(entitiesToMigrate.size, migratedEntities.size)
    migratedEntities.forEach { entity -> assertEquals("MIGRATED", entity.data.text) }

    val notMigratedEntities = exampleRepo.listByIds(entitiesNotToMigrate.map { it.id })
    assertEquals(entitiesNotToMigrate.size, notMigratedEntities.size)
    notMigratedEntities.forEach { entity -> assertEquals("SHOULD_NOT_MIGRATE", entity.data.text) }
  }

  @Test
  @Order(4)
  fun `migrateEntitySerialization can change serialized form of entities`() {
    @Serializable
    data class OldEntity(override val id: ExampleId, val oldField: Int) : Entity<ExampleId>

    @Serializable
    data class NewEntity(override val id: ExampleId, val newField: Int) : Entity<ExampleId>

    val oldSerializationAdapter = KotlinSerialization(OldEntity.serializer())
    val newSerializationAdapter = KotlinSerialization(NewEntity.serializer())

    val oldRepo =
        RepositoryJdbi<ExampleId, OldEntity>(
            jdbi,
            tableName = "example",
            serializationAdapter = oldSerializationAdapter,
        )
    val newRepo =
        RepositoryJdbi<ExampleId, NewEntity>(
            jdbi,
            tableName = "example",
            serializationAdapter = newSerializationAdapter,
        )

    val oldEntities =
        oldRepo.batchCreate(
            (1..500).map { number -> OldEntity(id = ExampleId(), oldField = number) },
        )

    @Suppress("ClassName") // Flyway expects this naming convention
    class V001_4__Migrate_serialization : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntitySerialization(
            context.connection,
            tableName = "example",
            currentSerializationAdapter = oldSerializationAdapter,
            targetSerializationAdapter = newSerializationAdapter,
            transform = { (oldEntity, _) ->
              NewEntity(oldEntity.id, newField = oldEntity.oldField)
            },
        )
      }
    }

    runMigration(V001_4__Migrate_serialization())

    val newEntities = newRepo.listAll()
    newEntities.shouldNotBeEmpty()
    newEntities.shouldHaveSize(oldEntities.size)
    newEntities
        .map { it.data }
        .shouldContainExactlyInAnyOrder(
            oldEntities.map { (oldEntity, _) ->
              NewEntity(oldEntity.id, newField = oldEntity.oldField)
            },
        )
  }

  // Disabled for automatic tests, since this is a heavy test that we don't want to run in CI
  @Disabled
  @Test
  @Order(5)
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
    class V001_5__Load_test_migration : BaseJavaMigration() {
      override fun migrate(context: Context) {
        migrateEntity(
            context.connection,
            tableName = "example",
            serializationAdapter = KotlinSerialization(LargeEntityPostMigration.serializer()),
        )
      }
    }

    runMigration(V001_5__Load_test_migration())
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
        // We can re-use the main example table here, since we truncate it between every test
        tableName = "example",
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
