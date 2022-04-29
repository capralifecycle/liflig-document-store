package no.liflig.documentstore

import no.liflig.documentstore.dao.AbstractCrudDao
import org.jdbi.v3.core.Jdbi

class ExampleDao(
  jdbi: Jdbi
) : AbstractCrudDao<ExampleId, ExampleAggregate>(
  jdbi,
  "example",
  ExampleSerializationAdapter()
)
