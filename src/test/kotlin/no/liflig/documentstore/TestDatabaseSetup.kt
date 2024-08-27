package no.liflig.documentstore

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin
import org.testcontainers.containers.PostgreSQLContainer

internal class AppPostgresSQLContainer :
    PostgreSQLContainer<AppPostgresSQLContainer>("postgres:16")

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

internal fun createTestDatabase(): Jdbi {
  val pgContainer = AppPostgresSQLContainer()

  pgContainer.withDatabaseName("example").withUsername("user").withPassword("pass").start()

  return createJdbiInstanceAndMigrate(createDataSource(pgContainer.jdbcUrl, "user", "pass"))
}
