package no.liflig.documentstore

import io.kotest.matchers.collections.shouldHaveSize
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.SearchRepositoryJdbi
import no.liflig.documentstore.examples.ExampleEntity
import no.liflig.documentstore.examples.ExampleQuery
import no.liflig.documentstore.examples.ExampleSerializationAdapter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DomainFilterTest {
  val jdbi = createTestDatabase()

  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

  val searchRepository = SearchRepositoryJdbi(jdbi, "example", serializationAdapter)

  val mockAdapter: ExampleSerializationAdapter = mockk {
    every { fromJson(any()) } returns ExampleEntity.create("")
  }
  val searchRepositoryWithMock = SearchRepositoryJdbi(jdbi, "example", mockAdapter)

  @BeforeEach
  fun clearDatabase() {
    searchRepository.search(ExampleQuery()).forEach {
      dao.delete(it.item.id, it.version)
    }
  }
  @Test
  fun domainFilterWorks() {
    runBlocking {
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      searchRepository.search(
        ExampleQuery(
          domainFilter = { it.text.contains("Hello") },
        )
      ) shouldHaveSize 2
    }
  }

  @Test
  fun limitRunsDeserializingCorrectAmountOfTimes() {
    runBlocking {
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      val result = searchRepositoryWithMock.search(
        ExampleQuery(
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
      dao.create(ExampleEntity.create("Hello Tes"))
      dao.create(ExampleEntity.create("Hello Alfred"))
      dao.create(ExampleEntity.create("Bye Ted"))
      dao.create(ExampleEntity.create("Bye Alfred"))

      val result = searchRepository.search(
        ExampleQuery(
          offset = 3
        )
      )

      result shouldHaveSize 1
    }
  }
}
