package no.liflig.documentstore.testutils

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import no.liflig.documentstore.DocumentStorePlugin
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.testcontainers.containers.PostgreSQLContainer

val dataSource: DataSource by lazy {
  val pgContainer = PostgreSQLContainer("postgres:16")
  pgContainer.withDatabaseName("example").withUsername("user").withPassword("pass").start()

  createDataSource(pgContainer.jdbcUrl, "user", "pass")
}

val jdbi: Jdbi by lazy { createJdbiInstanceAndMigrate(dataSource) }

val exampleRepo: ExampleRepository by lazy { ExampleRepository(jdbi, tableName = "example") }

/**
 * Separate table, to avoid other tests interfering with the count returned by
 * getByPredicateWithTotalCount.
 */
val exampleRepoWithCount: ExampleRepository by lazy {
  ExampleRepository(jdbi, tableName = "example_with_count")
}

/** Separate table, to avoid other tests interfering with the list returned by listAll. */
val exampleRepoForListAll: ExampleRepository by lazy {
  ExampleRepository(jdbi, tableName = "example_for_list_all")
}

val exampleRepoWithStringId: ExampleRepositoryWithStringEntityId by lazy {
  ExampleRepositoryWithStringEntityId(jdbi)
}

val exampleRepoWithIntegerId: ExampleRepositoryWithIntegerEntityId by lazy {
  ExampleRepositoryWithIntegerEntityId(jdbi)
}

val exampleRepoWithGeneratedIntegerId: ExampleRepositoryWithGeneratedIntegerEntityId by lazy {
  ExampleRepositoryWithGeneratedIntegerEntityId(jdbi)
}

const val MIGRATION_TABLE = "example_for_migration"

val exampleRepoPreMigration: ExampleRepository by lazy {
  ExampleRepository(jdbi, tableName = MIGRATION_TABLE)
}

val exampleRepoPostMigration: ExampleRepositoryForMigration by lazy {
  ExampleRepositoryForMigration(jdbi)
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
  val jdbi = Jdbi.create(dataSource).installPlugin(DocumentStorePlugin())

  Flyway.configure()
      .baselineOnMigrate(true)
      .baselineDescription("firstInit")
      .dataSource(dataSource)
      .locations("migrations")
      .load()
      .migrate()

  return jdbi
}
