package no.liflig.documentstore

import io.kotest.matchers.collections.shouldHaveSize
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.SearchRepositoryJdbi
import no.liflig.documentstore.examples.ExampleQuery
import no.liflig.documentstore.examples.ExampleSerializationAdapter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import no.liflig.documentstore.examples.ExampleEntity.Companion.create as createEntity

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DomainFilterTest {
  val jdbi = createTestDatabase()

  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

  val searchRepository = SearchRepositoryJdbi(jdbi, "example", serializationAdapter)

  val mockAdapter: ExampleSerializationAdapter = mockk {
    every { fromJson(any()) } returns createEntity("")
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
      dao.create(createEntity("Hello Tes"))
      dao.create(createEntity("Hello Alfred"))
      dao.create(createEntity("Bye Ted"))
      dao.create(createEntity("Bye Alfred"))

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
      dao.create(createEntity("Hello Tes"))
      dao.create(createEntity("Hello Alfred"))
      dao.create(createEntity("Bye Ted"))
      dao.create(createEntity("Bye Alfred"))

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
      dao.create(createEntity("Hello Tes"))
      dao.create(createEntity("Hello Alfred"))
      dao.create(createEntity("Bye Ted"))
      dao.create(createEntity("Bye Alfred"))

      val result = searchRepository.search(
        ExampleQuery(
          offset = 3
        )
      )

      result shouldHaveSize 1
    }
  }
}
