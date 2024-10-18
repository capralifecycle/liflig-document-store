@file:UseSerializers(InstantSerializer::class, UUIDSerializer::class)

package no.liflig.documentstore.testutils

import java.util.UUID
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import no.liflig.documentstore.entity.Entity
import no.liflig.documentstore.entity.StringEntityId
import no.liflig.documentstore.entity.UuidEntityId

@Serializable
data class ExampleEntity(
    override val id: ExampleId = ExampleId(),
    val text: String,
    val moreText: String? = null,
    val uniqueField: Int? = null,
) : Entity<ExampleId>

@Serializable
@JvmInline
value class ExampleId(override val value: UUID = UUID.randomUUID()) : UuidEntityId

@Serializable
data class EntityWithStringId(
    override val id: ExampleStringId,
    val text: String,
    val moreText: String? = null,
) : Entity<ExampleStringId>

@Serializable @JvmInline value class ExampleStringId(override val value: String) : StringEntityId

@Serializable
data class MigratedExampleEntity(
    override val id: ExampleId = ExampleId(),
    val text: String,
    val moreText: String? = null,
    val uniqueField: Int? = null,
    val newFieldAfterMigration: String? = null,
) : Entity<ExampleId>
