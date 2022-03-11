package no.liflig.documentstore

import no.liflig.documentstore.repository.AbstractCrudRepository
import org.jdbi.v3.core.Jdbi

class ExampleRepository(
  jdbi: Jdbi
) : AbstractCrudRepository<ExampleId, ExampleAggregate>(
  jdbi,
  "example",
  ExampleAggregate.serializer()
)
