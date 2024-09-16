package no.liflig.documentstore.testutils

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.testutils.examples.ExampleRepository
import no.liflig.documentstore.testutils.examples.ExampleRepositoryForMigration
import no.liflig.documentstore.testutils.examples.ExampleRepositoryWithStringEntityId
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin
import org.testcontainers.containers.PostgreSQLContainer

val jdbi: Jdbi by lazy { createTestDatabase() }

val exampleRepo: ExampleRepository by lazy { ExampleRepository(jdbi, tableName = "example") }

/**
 * Separate table, to avoid other tests interfering with the count returned by
 * getByPredicateWithTotalCount.
 */
val exampleRepoWithCount: ExampleRepository by lazy {
  ExampleRepository(jdbi, tableName = "example_with_count")
}

val exampleRepoWithStringId: ExampleRepositoryWithStringEntityId by lazy {
  ExampleRepositoryWithStringEntityId(jdbi)
}

val exampleRepoPreMigration: ExampleRepository by lazy {
  ExampleRepository(jdbi, tableName = "example_for_migration")
}

val exampleRepoPostMigration: ExampleRepositoryForMigration by lazy {
  ExampleRepositoryForMigration(jdbi)
}

private fun createTestDatabase(): Jdbi {
  val pgContainer = PostgreSQLContainer("postgres:16")
  pgContainer.withDatabaseName("example").withUsername("user").withPassword("pass").start()

  return createJdbiInstanceAndMigrate(createDataSource(pgContainer.jdbcUrl, "user", "pass"))
}

private fun createDataSource(
    jdbcUrl: String,
    username: String,
    password: String
): HikariDataSource {
  val config = HikariConfig()
  config.jdbcUrl = jdbcUrl
  config.driverClassName = "org.postgresql.Driver"
  config.username = username
  config.password = password

  return HikariDataSource(config)
}

private fun createJdbiInstanceAndMigrate(dataSource: DataSource): Jdbi {
  val jdbi: Jdbi =
      Jdbi.create(dataSource)
          .installPlugin(KotlinPlugin())
          .installPlugin(PostgresPlugin())
          .installPlugin(DocumentStorePlugin())

  Flyway.configure()
      .baselineOnMigrate(true)
      .baselineDescription("firstInit")
      .dataSource(dataSource)
      .locations("db/migrations")
      .load()
      .migrate()

  return jdbi
}
