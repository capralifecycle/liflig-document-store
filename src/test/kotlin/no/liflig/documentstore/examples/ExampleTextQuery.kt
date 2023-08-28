package no.liflig.documentstore.examples

import no.liflig.documentstore.dao.QueryObject
import org.jdbi.v3.core.statement.Query

class ExampleTextQuery(
  val text: String? = null,
  override val limit: Int? = null,
  override val offset: Int? = null,
  override val orderBy: String? = null,
  override val orderDesc: Boolean = false,
) : QueryObject() {
  override val sqlWhere: String = ":wildcardQuery IS NULL OR data->>'text' ILIKE :wildcardQuery "
  override val bindSqlParameters: Query.() -> Query = { bind("wildcardQuery", text?.let { "%$it%" }) }
}
