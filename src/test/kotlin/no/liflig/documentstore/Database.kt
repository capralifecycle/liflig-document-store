package no.liflig.documentstore

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import no.liflig.documentstore.entity.EntityId
import no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory
import no.liflig.documentstore.entity.UuidEntityIdArgumentFactory
import no.liflig.documentstore.entity.VersionArgumentFactory
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin
import org.testcontainers.containers.PostgreSQLContainer

class AppPostgresSQLContainer : PostgreSQLContainer<AppPostgresSQLContainer>("postgres:16")

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
          .registerArgument(UuidEntityIdArgumentFactory())
          .registerArgument(UnmappedEntityIdArgumentFactory())
          .registerArgument(VersionArgumentFactory())
          .registerArrayType(EntityId::class.java, "uuid")

  Flyway.configure()
      .baselineOnMigrate(true)
      .baselineDescription("firstInit")
      .dataSource(dataSource)
      .locations("db/migrations")
      .load()
      .migrate()

  return jdbi
}

fun createTestDatabase(): Jdbi {
  val pgContainer = AppPostgresSQLContainer()

  pgContainer.withDatabaseName("example").withUsername("user").withPassword("pass").start()

  return createJdbiInstanceAndMigrate(createDataSource(pgContainer.jdbcUrl, "user", "pass"))
}
