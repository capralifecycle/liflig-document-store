package no.liflig.documentstore

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SearchRepositoryTest {
  val jdbi = createTestDatabase()
  val serializationAdapter = ExampleSerializationAdapter()
  val dao = CrudDaoJdbi(jdbi, "example", serializationAdapter)

  val searchRepository =
    SearchRepositoryJdbi<ExampleId, ExampleEntity, ExampleQuery>(jdbi, "example", serializationAdapter)

  @BeforeEach
  fun clearDatabase() {
    searchRepository.search(ExampleQuery()).forEach {
      dao.delete(it.item.id, it.version)
    }
  }

  @Test
  fun testWhere() {
    runBlocking {
      dao.create(ExampleEntity.create("hello world"))
      dao.create(ExampleEntity.create("world"))

      val result = searchRepository.search(ExampleQuery(text = "hello world"))

      result shouldHaveSize 1
    }
  }

  @Test
  fun testLimit() {
    runBlocking {
      dao.create(ExampleEntity.create("1"))
      dao.create(ExampleEntity.create("2"))
      dao.create(ExampleEntity.create("3"))

      val result = searchRepository.search(ExampleQuery(limit = 2))

      result shouldHaveSize 2
    }
  }

  @Test
  fun testOffsett() {
    runBlocking {
      dao.create(ExampleEntity.create("hello world"))
      dao.create(ExampleEntity.create("world"))
      dao.create(ExampleEntity.create("world"))

      val result = searchRepository.search(ExampleQuery(offset = 1))

      result shouldHaveSize 2
    }
  }

  @Test
  fun emptySearchReturnsAllElements() {
    runBlocking {
      dao.create(ExampleEntity.create("A"))
      dao.create(ExampleEntity.create("B"))
      dao.create(ExampleEntity.create("C"))

      val result = searchRepository.search(ExampleQuery())

      result shouldHaveSize 3
    }
  }

  @Test
  fun orderAscWorks() {
    runBlocking {
      dao.create(ExampleEntity.create("A"))
      dao.create(ExampleEntity.create("B"))
      dao.create(ExampleEntity.create("C"))

      val result = searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)).map { it.item.text }

      result shouldBeEqual listOf("A", "B", "C")
    }
  }
  @Test
  fun orderDescWorksBothWays() {
    runBlocking {
      dao.create(ExampleEntity.create("A"))
      dao.create(ExampleEntity.create("B"))
      dao.create(ExampleEntity.create("C"))

      val result = searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = false)).map { it.item.id }
      val resul2 = searchRepository.search(ExampleQuery(orderBy = "data->>'text'", orderDesc = true)).map { it.item.id }

      result shouldBeEqual resul2.asReversed()
    }
  }

  @Test
  fun domainFilterWorks() {
    runBlocking {
      dao.create(ExampleEntity.create("A"))
      dao.create(ExampleEntity.create("B"))
      dao.create(ExampleEntity.create("C"))

      val result = searchRepository.search(ExampleQuery(domainFilter = { it.text == "B" }))

      result shouldHaveSize 1
      result.first().item.text shouldBeEqual "B"
    }
  }
}
