package no.liflig.documentstore

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.ints.exactly
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.SearchRepositoryWithCountJdbi
import no.liflig.documentstore.testexamples.ExampleEntity
import no.liflig.documentstore.testexamples.ExampleId
import no.liflig.documentstore.testexamples.ExampleQueryObject
import no.liflig.documentstore.testexamples.ExampleSearchRepository
import no.liflig.documentstore.testexamples.ExampleSerializationAdapter
import no.liflig.documentstore.testexamples.ExampleTextSearchQuery
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DomainPredicateTest {
  val jdbi = createTestDatabase()

  val serializationAdapter = ExampleSerializationAdapter()

  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

  val searchRepository = ExampleSearchRepository(jdbi, "example", serializationAdapter)

  val searchRepositoryWithCountJdbi = SearchRepositoryWithCountJdbi<ExampleId, ExampleEntity, ExampleTextSearchQuery>(
    jdbi, "example", serializationAdapter,
  )

  @BeforeEach
  fun clearDatabase(){
    searchRepository.listAll().forEach {
      dao.delete(it.item.id, it.version)
    }
  }
  @Test
  fun domainPredicateWorks() {
    runBlocking {
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      searchRepository.search(
        ExampleQueryObject(
          domainPredicate = { it.text.contains("Hello") },
        )
      ) shouldHaveSize 2
    }
  }

  @Test
  fun limitWorks() {
    runBlocking {
      val mockAdapter: ExampleSerializationAdapter = mockk {
        every { fromJson(any()) } returns ExampleEntity.create("")
      }
      val searchRepository = ExampleSearchRepository(jdbi, "example", mockAdapter)
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      val result = searchRepository.search(
        ExampleQueryObject(
          limit = 1
        )
      )

      result shouldHaveSize 1
      verify(exactly = 1) { mockAdapter.fromJson(any()) }
    }
  }

  @Test
  fun offSetWorks() {
    runBlocking {
      val mockAdapter: ExampleSerializationAdapter = mockk {
        every { fromJson(any()) } returns ExampleEntity.create("")
      }
      val searchRepository = ExampleSearchRepository(jdbi, "example", mockAdapter)
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      val result = searchRepository.search(
        ExampleQueryObject(
          offset = 3
        )
      )

      result shouldHaveSize 1
    }
  }
}
