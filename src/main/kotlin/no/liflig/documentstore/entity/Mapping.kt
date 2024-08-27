@file:Suppress("DeprecatedCallableAddReplaceWith")

package no.liflig.documentstore.entity

import java.util.*

@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
typealias StringMapper<T> = (String) -> T

/** Parse a [String] into [UUID] with error handling. */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
fun parseUuid(value: String): UUID = UUID.fromString(value)

/**
 * Create a mapper function to convert a [String] holding an [UUID] into a known [T].
 *
 * [IllegalArgumentException] in the conversion will be handled.
 */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
fun <T> createUuidMapper(factory: (UUID) -> T): StringMapper<T> = { factory.invoke(parseUuid(it)) }

/**
 * Create a pair representing the mapping of a specific [T] from an [UUID] stored in a [String] by
 * using the provided [factory] function.
 *
 * [IllegalArgumentException] in the conversion will be handled.
 */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
@JvmName("createMapperPairForUuid")
inline fun <reified T> createMapperPairForUuid(
    noinline factory: (UUID) -> T
): Pair<Class<T>, StringMapper<T>> = T::class.java to createUuidMapper(factory)

/**
 * Create a pair representing the mapping of a specific [T] from a [String] by using the provided
 * [factory] function.
 *
 * [IllegalArgumentException] in the conversion will be handled.
 */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
@JvmName("createMapperPairForString")
inline fun <reified T> createMapperPairForString(
    noinline factory: (String) -> T
): Pair<Class<T>, StringMapper<T>> = T::class.java to factory

/**
 * Create a pair representing the mapping of a specific [T] from a [String] by using the provided
 * [factory] function.
 */
@Deprecated(
    "This is being removed in an upcoming version of Liflig Document Store, since we found no users of it.",
    level = DeprecationLevel.WARNING,
)
@JvmName("createMapperPairForStringMapper")
inline fun <reified T> createMapperPairForStringMapper(
    noinline factory: StringMapper<T>
): Pair<Class<T>, StringMapper<T>> = T::class.java to factory
