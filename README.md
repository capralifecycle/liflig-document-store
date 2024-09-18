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

## Usage

See [docs/usage.md](docs/usage.md).

## Contributing

To check build before pushing:

```bash
mvn verify
```

GitHub Actions will automatically release a new version for commits on master.
