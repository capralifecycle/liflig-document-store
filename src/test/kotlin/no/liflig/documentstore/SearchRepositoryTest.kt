package no.liflig.documentstore

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import no.liflig.documentstore.dao.CrudDaoJdbi
import no.liflig.documentstore.dao.SearchRepositoryJdbi
import no.liflig.documentstore.examples.ExampleEntity
import no.liflig.documentstore.examples.ExampleId
import no.liflig.documentstore.examples.ExampleQuery
import no.liflig.documentstore.examples.ExampleSerializationAdapter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import no.liflig.documentstore.examples.ExampleEntity.Companion.create as createEntity

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SearchRepositoryTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

  val searchRepository =
    SearchRepositoryJdbi<ExampleId, ExampleEntity, ExampleQuery>(jdbi, "example", serializationAdapter)

  val mockAdapter: ExampleSerializationAdapter = mockk {
    every { fromJson(any()) } returns createEntity("")
  }
  val searchRepositoryWithMock =
    SearchRepositoryJdbi<ExampleId, ExampleEntity, ExampleQuery>(jdbi, "example", mockAdapter)

  @BeforeEach
  fun clearDatabase() {
    searchRepository.search(ExampleQuery()).forEach {
      dao.delete(it.item.id, it.version)
    }
  }

  @Test
  fun exampleQueryWorks() {
    runBlocking {
      dao.create(createEntity("hello world"))
      dao.create(createEntity("world"))

      val result = searchRepository.search(ExampleQuery(text = "hello world"))

      result shouldHaveSize 1
    }
  }

  @Test
  fun `limit returns correct amount of items`() {
    runBlocking {
      dao.create(createEntity("1"))
      dao.create(createEntity("2"))
      dao.create(createEntity("3"))

      val result = searchRepository.search(ExampleQuery(limit = 2))

      result shouldHaveSize 2
    }
  }

  @Test
  fun `offset skips right amount of items`() {
    runBlocking {
      dao.create(createEntity("hello world"))
      dao.create(createEntity("world"))
      dao.create(createEntity("world"))

      val result = searchRepository.search(ExampleQuery(offset = 1))

      result shouldHaveSize 2
    }
  }

  @Test
  fun `offset and limit returns correct items`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))
      dao.create(createEntity("D"))

      val result = searchRepository.search(ExampleQuery(offset = 1, limit = 2)).map { it.item.text }

      result shouldHaveSize 2
      result shouldBeEqual listOf("B", "C")
    }
  }

  @Test
  fun `offset and limit works with domain filter`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))
      dao.create(createEntity("D"))
      dao.create(createEntity("this-A"))
      dao.create(createEntity("this-B"))
      dao.create(createEntity("this-C"))
      dao.create(createEntity("this-D"))

      val result =
        searchRepository.searchDomainFiltered(
          ExampleQuery(
            offset = 1,
            limit = 2,
          )
        ) { it.text.startsWith("this") }
          .map { it.item.text }

      result shouldHaveSize 2
      result shouldBeEqual listOf("this-B", "this-C")
    }
  }

  @Test
  fun `empty search returns all items`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result = searchRepository.search(ExampleQuery())

      result shouldHaveSize 3
    }
  }

  @Test
  fun `orderBy orders correctly`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result =
        searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)).map { it.item.text }

      result shouldBeEqual listOf("A", "B", "C")
    }
  }

  @Test
  fun `orderBy orders correctly for domain filtered search`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result =
        searchRepository.searchDomainFiltered(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)) { true }
          .map { it.item.text }

      result shouldBeEqual listOf("A", "B", "C")
    }
  }

  @Test
  fun `orderDesc flips direction`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result =
        searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)).map { it.item.id }
      val resul2 = searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = true)).map { it.item.id }

      result shouldBeEqual resul2.asReversed()
    }
  }

  @Test
  fun `orderDesc flips direction in fomain filtered search`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result =
        searchRepository.searchDomainFiltered(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)) { true }
          .map { it.item.id }
      val resul2 =
        searchRepository.searchDomainFiltered(ExampleQuery(orderBy = "data->>'text'", orderDesc = true)) { true }
          .map { it.item.id }

      result shouldBeEqual resul2.asReversed()
    }
  }

  @Test
  fun `domain filter returns correct items`() {
    runBlocking {
      dao.create(createEntity("A"))
      dao.create(createEntity("B"))
      dao.create(createEntity("C"))

      val result = searchRepository.searchDomainFiltered(ExampleQuery()) { it.text == "B" }

      result shouldHaveSize 1
      result.first().item.text shouldBeEqual "B"
    }
  }

  @Test
  fun `deserializer runs exactly once when one match is found`() {
    runBlocking {
      dao.create(createEntity("Hello Tes"))
      dao.create(createEntity("Hello Alfred"))
      dao.create(createEntity("Bye Ted"))
      dao.create(createEntity("Bye Alfred"))

      val result = searchRepositoryWithMock.searchDomainFiltered(
        ExampleQuery(
          limit = 1
        )
      ) {
        true
      }

      result shouldHaveSize 1
      verify(exactly = 1) { mockAdapter.fromJson(any()) }
    }
  }

  @Test
  fun `offset works as intended`() {
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

  @Test
  fun `offset works as intended for search with domain filter`() {
    runBlocking {
      dao.create(createEntity("Hello Tes"))
      dao.create(createEntity("Hello Alfred"))
      dao.create(createEntity("Bye Ted"))
      dao.create(createEntity("Bye Alfred"))

      val result = searchRepository.searchDomainFiltered(
        ExampleQuery(
          offset = 3
        )
      ) {
        true
      }

      result shouldHaveSize 1
    }
  }
}
