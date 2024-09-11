# liflig-document-store

Kotlin library implementing the repository pattern for storing documents as JSONB in Postgres.

- CRUD-like repository with extendable methods for flexibility.
- Optimistic locking.
- JDBI library for convenience.
- Serialization and deserialization of objects using Kotlinx Serialization.
- Connection pool with HikariCP.
- Database migrations with Flyway.

This library is currently only distributed in Liflig internal repositories.

## Usage

See [docs/usage.md](docs/usage.md).

## Contributing

This project follows
https://confluence.capraconsulting.no/x/fckBC

To check build before pushing:

```bash
mvn verify
```

The CI server will automatically release new version for builds on master.
