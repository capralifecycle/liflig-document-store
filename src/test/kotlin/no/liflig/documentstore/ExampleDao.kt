package no.liflig.documentstore

import no.liflig.documentstore.dao.AbstractCrudDao
import no.liflig.documentstore.dao.SerializationAdapter
import org.jdbi.v3.core.Jdbi

class ExampleDao(
  jdbi: Jdbi,
  serializationAdapter: SerializationAdapter<ExampleEntity> = ExampleSerializationAdapter()
) : AbstractCrudDao<ExampleId, ExampleEntity>(
  jdbi,
  "example",
  serializationAdapter
)
