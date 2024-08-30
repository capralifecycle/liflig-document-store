# Liflig Document Store V2 Migration Guide

After internal discussion in Liflig, we have decided to make a series of changes to the library.
Some of these are backwards-compatible, while some are breaking changes. Our plan for this is to:

1. Mark APIs that we plan to move/remove/change with `@Deprecated`, and add the new replacement
   APIs.
   - Old APIs remain unchanged, so this is not a breaking change, but you will receive a warning
     when using these and a suggestion to replace with new APIs (in some cases, IntelliJ can do this
     replacement automatically).
2. Once sufficient time has been given for the migration period, remove the deprecated APIs and bump
   the major version to v2.

## Changed APIs

The following list goes through the APIs that are being deprecated in v2, and how to migrate away
from them.

### 1. `CrudDao` and `SearchDao` replaced by `Repository`

Previously, we implemented repositories via composition, using `CrudDao` and `SearchDao` as fields.
`CrudDao` had a default implementation through `CrudDaoJdbi`, while `SearchDao` had to be
implemented manually (though with some help from `AbstractSearchDao`). However, there were some
fundamental flaws with the `SearchDao` abstraction, that led to a lot of boilerplate code when
implementing it. In addition, we typically wanted our repositories to have all the methods from
`CrudDao` (`create`, `get`, `update`, `delete`). One should be careful not to abuse inheritance, but
this is a good use case for it: we can get default implementations for CRUD operations, and then
implement search/filter ourselves. So we have decided to use inheritance going forward, implementing
repositories by extending the new `RepositoryJdbi` class.

`RepositoryJdbi` is much like `CrudDaoJdbi`, except it also provides `getByPredicate` (from
`AbstractSearchDao`) and `getByPredicateWithTotalCount` (from `AbstractSearchDaoWithCount`) to
subclasses. This lets you implement search/filtering directly on the repository, instead of going
through the `SearchDao` abstraction.

Example of migrating:

- Old version:

```kt
class ExampleRepository(
  val crudDao: CrudDao<ExampleId, ExampleEntity>,
  val searchDao: SearchDao<ExampleId, ExampleEntity, ExampleSearchQuery>,
) {
  // CRUD methods

  fun getByName(name: String): VersionedEntity<ExampleEntity>? {
    return searchDao.search(ExampleSearchQuery(name)).firstOrNull()
  }
}

class ExampleSearchQuery(val name: String)

class ExampleSearchDao(jdbi: Jdbi, tableName: String) :
    AbstractSearchDao<ExampleId, ExampleEntity, ExampleSearchQuery>(
      jdbi,
      tableName,
      exampleSerializationAdapter,
    ) {
  override fun search(query: ExampleSearchQuery): List<VersionedEntity<ExampleEntity>> {
    return getByPredicate("data->>'name' = :name") {
      bind("name", query.name)
    }
  }
}

fun main() { // Or wherever you set up your repositories
  val exampleRepo = ExampleRepository(
    CrudDaoJdbi(jdbi, "example", exampleSerializationAdapter),
    ExampleSearchDao(jdbi, "example"),
  )
}
```

- New version:

```kt
class ExampleRepository(jdbi: Jdbi) :
    RepositoryJdbi<ExampleId, ExampleEntity>(
      jdbi,
      tableName = "example",
      exampleSerializationAdapter,
    ) {
  override fun getByName(name: String): Versioned<ExampleEntity>? {
    return getByPredicate("data->>'name' = :name") {
        bind("name", query.name)
      }
      .firstOrNull()
  }
}

fun main() { // Or wherever you set up your repositories
  val exampleRepo = ExampleRepository(jdbi)
}
```

### 2. Argument type registration with `DocumentStorePlugin`

If you previously registered argument types from Liflig Document Store manually when setting up
JDBI, you must now instead use the new `DocumentStorePlugin`. This makes it easier for us to migrate
argument type registration in the future, and potentially add new argument types to
`DocumentStorePlugin` without all library consumers having to manually add them themselves. For
example, `StringEntityId` was recently added to Document Store, but it requires registering the
`StringEntityIdArgumentFactory` to work - without this, queries will fail at runtime with reflection
errors. Since services so far have done manual argument type registration, most will not have this
argument type registered, and so may encounter runtime reflection errors if trying to use
`StringEntityId`. `DocumentStorePlugin` fixes this issue.

- Old argument type registration:

```kt
Jdbi.create(dataSource)
    .installPlugin(KotlinPlugin())
    .registerArgument(UuidEntityIdArgumentFactory())
    .registerArgument(UnmappedEntityIdArgumentFactory())
    .registerArgument(VersionArgumentFactory())
    .registerArrayType(EntityId::class.java, "uuid")
```

- New (using `DocumentStorePlugin`):

```kt
import no.liflig.documentstore.DocumentStorePlugin

Jdbi.create(dataSource)
    .installPlugin(KotlinPlugin())
    .installPlugin(DocumentStorePlugin())
```

### 3. `VersionedEntity<EntityT>` renamed to `Versioned<EntityT>`, with new fields

This has been renamed, since `VersionedEntity<EntityT>` was redundant and led to long function
signatures. In addition, we now also include `createdAt` and `modifiedAt` fields on
`Versioned<EntityT>`, since they are always included in the database tables expected by Liflig
Document Store, but we just didn't expose them previously.

It should be sufficient to just replace `VersionedEntity<EntityT>` with `Versioned<EntityT>`, and
IntelliJ should suggest this as an automatic fix.

### 4. `EntityRoot` replaced by `Entity`

`EntityRoot` was a redundant abstraction on top of the `Entity` interface. You should replace
uses of `EntityRoot` with `Entity`, and `AbstractEntityRoot` with `AbstractEntity` (IntelliJ should
suggest this as an automatic fix). All non-deprecated APIs have been updated to work with `Entity`
instead of `EntityRoot`.

### 5. Changed package locations and renames

As part of the change from `CrudDao`/`SearchDao` to `Repository`, we added a new package
`no.liflig.documentstore.repository` to replace the old `no.liflig.documentstore.dao` package.
Previous APIs remain under `dao`, but now have new versions in `repository`. Some have also been
renamed to reflect the change from `Dao` to `Repository`. These include the following:

- `no.liflig.documentstore.dao.SerializationAdapter`
  - -> `no.liflig.documentstore.repository.SerializationAdapter`
- `no.liflig.documentstore.dao.transactional`
  - -> `no.liflig.documentstore.repository.transactional`
- `no.liflig.documentstore.dao.getHandle`
  - -> `no.liflig.documentstore.repository.useHandle`
- `no.liflig.documentstore.dao.DaoException`
  - -> `no.liflig.documentstore.repository.RepositoryException`
- `no.liflig.documentstore.dao.ConflictDaoException`
  - -> `no.liflig.documentstore.repository.ConflictRepositoryException`
- `no.liflig.documentstore.dao.UnavailableDaoException`
  - -> `no.liflig.documentstore.repository.UnavailableRepositoryException`
- `no.liflig.documentstore.dao.ListWithTotalCount`
  - -> `no.liflig.documentstore.repository.ListWithTotalCount`

IntelliJ should be able to automatically fix these.

### 6. APIs that are being removed/no longer public

In addition to `CrudDao`/`SearchDao`, there are some other APIs in Liflig Document Store that were
likely made public by accident, and should really be internal. Also, there were APIs that were never
used, or added as just as an experiment. The following list is being deprecated, and is slated for
removal in v2. If you depend on any of these, give a heads-up to the Liflig developers.

- `no.liflig.documentstore.dao.EntityRow`
- `no.liflig.documentstore.dao.createRowMapper`
- `no.liflig.documentstore.dao.createRowParser`
- `no.liflig.documentstore.dao.mapExceptions`
- `no.liflig.documentstore.dao.EntityRowWithCount`
- `no.liflig.documentstore.dao.MappedEntityWithCount`
- `no.liflig.documentstore.dao.createRowMapperWithCount`
- `no.liflig.documentstore.dao.createRowParserWithCount`
- `no.liflig.documentstore.entity.EntityList`
  - This type alias was introduced to alleviate the long type signatures from `VersionedEntity`,
    which has now been renamed. You should use `List<Versioned<EntityT>>` instead.
  - The extension functions on `EntityList` are now instead defined on `List<Versioned<EntityT>>`.
- `no.liflig.documentstore.entity.EntityListWithTotalCount`
  - Same as `EntityList`: use `ListWithTotalCount<Versioned<EntityT>>` instead.
- `no.liflig.documentstore.entity.EntityTimestamps`
- `no.liflig.documentstore.entity.StringMapper`
- `no.liflig.documentstore.entity.parseUuid`
- `no.liflig.documentstore.entity.createUuidMapper`
- `no.liflig.documentstore.entity.createMapperPairForUuid`
- `no.liflig.documentstore.entity.createMapperPairForString`
- `no.liflig.documentstore.entity.createMapperPairForStringMapper`
- `no.liflig.documentstore.entity.UnmappedEntityId`
- `no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory`
  - See point 2 above
- `no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory`
  - See point 2 above
- `no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory`
  - See point 2 above
- `no.liflig.documentstore.entity.UnmappedEntityIdArgumentFactory`
  - See point 2 above
- `no.liflig.documentstore.registerLifligArgumentTypes`
  - See point 2 above
