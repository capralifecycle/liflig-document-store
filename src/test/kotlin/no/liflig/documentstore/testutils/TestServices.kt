package no.liflig.documentstore.testutils

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import no.liflig.documentstore.DocumentStorePlugin
import no.liflig.documentstore.repository.useHandle
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.testcontainers.containers.PostgreSQLContainer

val dataSource: DataSource by lazy {
  val pgContainer = PostgreSQLContainer("postgres:16")
  pgContainer.withDatabaseName("example").withUsername("user").withPassword("pass").start()

  createDataSource(pgContainer.jdbcUrl, "user", "pass")
}

val jdbi: Jdbi by lazy { createJdbiInstanceAndMigrate(dataSource) }

fun clearDatabase() {
  useHandle(jdbi) { handle ->
    handle.execute("""TRUNCATE TABLE "example"""")
    handle.execute("""TRUNCATE TABLE "example_with_string_id"""")
    handle.execute("""TRUNCATE TABLE "example_with_integer_id"""")
    handle.execute("""TRUNCATE TABLE "example_with_generated_integer_id"""")
  }
}

val exampleRepo: ExampleRepository by lazy { ExampleRepository(jdbi) }

val exampleRepoWithStringId: ExampleRepositoryWithStringEntityId by lazy {
  ExampleRepositoryWithStringEntityId(jdbi)
}

val exampleRepoWithIntegerId: ExampleRepositoryWithIntegerEntityId by lazy {
  ExampleRepositoryWithIntegerEntityId(jdbi)
}

val exampleRepoWithGeneratedIntegerId: ExampleRepositoryWithGeneratedIntegerEntityId by lazy {
  ExampleRepositoryWithGeneratedIntegerEntityId(jdbi)
}

val exampleRepoPostMigration: ExampleRepositoryAfterMigration by lazy {
  ExampleRepositoryAfterMigration(jdbi)
}

private fun createDataSource(
    jdbcUrl: String,
    username: String,
    password: String,
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
