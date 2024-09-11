# Using Liflig Document Store

Liflig Document Store works on the assumption that all database tables look alike, using the `jsonb`
column type from Postgres to store data. So you first have to create your table like this:

```sql
CREATE TABLE example
(
  -- Can have type `text` if using `StringEntityId`
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
)
```

Then, define an entity in your application. If you use `kotlinx-serialization`, the entity must be
`@Serializable`.

```kotlin
import kotlinx.serialization.Serializable
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.UuidEntityId

@Serializable
data class ExampleEntity(
    override val id: ExampleId,
    val name: String,
) : Entity<ExampleId>

@Serializable
@JvmInline
value class ExampleId(override val value: UUID) : UuidEntityId {
  override fun toString() = value.toString()
}
```

Next, implement a `Repository` for your entity. Here we use the `getByPredicate` method on
`RepositoryJdbi` to provide our own `WHERE` clause in `getByName`. See below for the implementation
of [`KotlinSerialization`](#implementing-serializationadapter).

```kotlin
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.RepositoryJdbi
import org.jdbi.v3.core.Jdbi

class ExampleRepository(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, ExampleEntity>(
        jdbi,
        tableName = "example",
        KotlinSerialization(ExampleEntity.serializer()),
    ) {

  fun getByName(name: String): Versioned<ExampleEntity>? {
    // Here we use JSON operators from Postgres to query on the entity's fields
    return getByPredicate("data->>'name' = :name") {
          bind("name", name)
        }
        .firstOrNull()
  }
}
```

This inherits CRUD methods (`create`, `get`, `update` and `delete`) from `RepositoryJdbi`, so we can
use it like this:

```kotlin
val exampleRepo: ExampleRepository

val (entity, version) = exampleRepo.create(
    ExampleEntity(
        id = ExampleId(UUID.randomUUID()),
        name = "test",
    )
)

println("Created entity with name '${entity.name}'")
```

Finally, when setting up our `Jdbi` instance to connect to our database, we have to install the
`DocumentStorePlugin` (this registers argument types that `RepositoryJdbi` depends on):

```kotlin
import javax.sql.DataSource
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin

fun createJdbiInstance(dataSource: DataSource): Jdbi {
  return Jdbi.create(dataSource)
      .installPlugin(KotlinPlugin())
      .installPlugin(DocumentStorePlugin())
}
```

## Implementing `SerializationAdapter`

The `KotlinSerialization` class used in the `ExampleRepository` above is an implementation of the
`SerializationAdapter` interface from Liflig Document Store. This interface allows you to choose
your own JSON serialization library when using Document Store. Here is an example implementation
for `kotlinx-serialization`:

```kotlin
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.repository.SerializationAdapter

private val json = Json {
  encodeDefaults = true
  ignoreUnknownKeys = true
}

class KotlinSerialization<EntityT : Entity<*>>(
    private val serializer: KSerializer<EntityT>,
) : SerializationAdapter<EntityT> {
  override fun toJson(entity: EntityT): String = json.encodeToString(serializer, entity)

  override fun fromJson(value: String): EntityT = json.decodeFromString(serializer, value)
}
```
