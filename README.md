# Liflig Document Store

Kotlin library implementing the repository pattern for storing documents as JSONB in Postgres.

Provides:

- Extendible repository class, with default implementations of CRUD operations
- Optimistic locking
- Transaction management
- Migration utilities

Document Store uses the [JDBI library](https://jdbi.org/releases/3.45.1/#_introduction_to_jdbi_3)
for convenient database access.

This library is currently only distributed in Liflig internal repositories.

**Contents:**

- [Usage](#usage)
  - [Implementing `SerializationAdapter`](#implementing-serializationadapter)
- [Optimistic locking](#optimistic-locking)
  - [Avoiding `ConflictRepositoryException`](#avoiding-conflictrepositoryexception)
- [Contributing](#contributing)

## Usage

Liflig Document Store works on the assumption that all database tables look alike, using the `jsonb`
column type from Postgres to store data. So you first have to create your table like this:

```sql
CREATE TABLE example
(
  -- Can have type `text` if using `StringEntityId`, or `bigint` if using `IntegerEntityId`
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

Next, implement a `Repository` for your entity. See below for the implementation of
[`KotlinSerialization`](#implementing-serializationadapter).

```kotlin
import no.liflig.documentstore.entity.Versioned
import no.liflig.documentstore.repository.RepositoryJdbi
import org.jdbi.v3.core.Jdbi

class ExampleRepository(jdbi: Jdbi) :
  RepositoryJdbi<ExampleId, ExampleEntity>(
    jdbi,
    tableName = "example",
    serializationAdapter = KotlinSerialization(ExampleEntity.serializer()),
  ) {

  fun getByName(name: String): Versioned<ExampleEntity>? {
    // To implement filtering on the entity's fields, we use the getByPredicate method inherited
    // from RepositoryJdbi. It lets us provide our own WHERE clause, and a lambda to bind arguments.
    // In the WHERE string, we use JSON operators from Postgres (->>) to query the entity's fields.
    return getByPredicate("data->>'name' = :name") {
      // Binds to the :name parameter in the query
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

### Implementing `SerializationAdapter`

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

## Optimistic locking

When using a database as a document store, the application becomes the single source of truth for
the schema of the data (since the schema in the database is just an unspecified `jsonb` blob).
Because of this, it becomes extra important that the application has control over how database
entities are mutated. This can be compromised when we have concurrent updates of entities in our
application, as follows:

```
    Process 1          Process 2
       |                  |
       |                  |
   Get entity             |           <-- Entity state 1
       |                  |
       |            Get same entity   <-- Entity state 1
       |                  |
       |                  |
 Update entity            |           <-- Entity state 2
       |                  |
       |                  |
       |            Update entity     <-- Entity state 3
       |                  |
       |                  |       
```

The application may enforce domain rules when updating an entity, which specifies what state
transitions are valid for an entity. In the example above, we have a Process 1 that gets the entity
and performs an update on it, transitioning the entity from "state 1" to "state 2". And then we have
a concurrent Process 2, which gets the same entity, and then also applies an update to it - but
since it fetched the entity before Process 1 completed its update, Process 2 will transition the
entity from "state 1" to "state 3"! Not only does this overwrite the changes from Process 1, but it
may also break domain rules about what are valid state transitions for an entity.

In order to avoid this, Document Store implements "optimistic locking". This works by
attaching a `version` number to every entity (part of the common table schema for all entities, see
[Usage](#usage)), which starts at 1 and increments every time an entity is updated. When fetching
an entity, this version number is returned (the caller gets a `Versioned<Entity>` instead of just
an `Entity`). When calling `Repository.update`, the caller must provide this version as an argument.
If the version matches the current version of the entity in the database, the update is performed
and the entity's version is incremented. If the version does _not_ match, then a
`ConflictRepositoryException` is thrown, with a message describing that the entity was concurrently
modified.

### Avoiding `ConflictRepositoryException`

How to avoid `ConflictRepositoryException` depends on how your application wants to deal with
concurrent updates. For example, you may return an entity's version out to a web client, that is
used by a human user to update the entity in a form. Upon submitting the form, you can then attach
the entity version (using an
[`ETag` header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/ETag), for
example), and use that in the call to `Repository.update`. If a `ConflictRepositoryException` is
thrown, you can catch that and map it to a Bad Request response to the user describing that another
user updated the entity simultaneously, and that they will have to refresh to get the latest
changes.

Another alternative, if the fetch + update are performed after each other in the same server
process (not returned out to a user in between), is to lock the entity's row in the database while
performing the update. This can be achieved with the
[`FOR UPDATE`](https://www.cockroachlabs.com/blog/select-for-update/) SQL clause, which locks rows
returned by `SELECT` for the duration of a transaction, blocking other processes from reading the
rows (makes them wait) until the transaction completes. Document Store has special support for this,
through the `forUpdate` boolean parameter in `Repository.get`, `Repository.listByIds` and
`RepositoryJdbi.getByPredicate`. In order to use it, you set `forUpdate = true` when calling these
methods, and wrap your fetch + update in the `transactional` scope function in order to run it in a
database transaction (required for `FOR UPDATE` to work):

<!-- @formatter:off -->
```kotlin
exampleRepository.transactional {
  val (exampleEntity, version) = exampleRepository.getOrThrow(exampleId, forUpdate = true)
  
  exampleRepository.update(exampleEntity.copy(name = newName), version)
}
```
<!-- @formatter:on -->

Using this technique, all entity fetches + updates are performed atomically, so our application gets
control of all entity state transition and can apply domain rules. So the above diagram becomes as
follows:

```
    Process 1          Process 2
       |                  |
       |                  |
   Transaction            |
     start                |
       |                  |
   Get entity             |
   FOR UPDATE             |
       |                  |
       |                  |
       |              Transaction
       |                start
       |                  |
       |            Get same entity
       |         Blocked by FOR UPDATE
       |                  |
       |                  |
 Update entity            |
       |                  |
   Transaction            |
     commit               |
       |                  |
       |                  |
       |           'Get same entity'
       |        returns, with Process 1's
       |            update applied
       |                  |
       |            Update entity
       |                  |
       |              Transaction
       |                commit
       |                  |
       |                  |
```

## Contributing

To check build before pushing:

```bash
mvn verify
```

GitHub Actions will automatically release a new version for commits on master.
