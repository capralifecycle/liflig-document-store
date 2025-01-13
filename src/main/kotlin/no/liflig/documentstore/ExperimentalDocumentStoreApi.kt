// We want to keep this around even if it's unused, for future experimental APIs.
@file:Suppress("unused")

package no.liflig.documentstore

@RequiresOptIn(
    """
      This API is currently under development in Liflig Document Store, and is experimental.
      You must explicitly opt in to it with @OptIn(ExperimentalDocumentStoreApi::class) to use it.
      Be aware that the API may have breaking changes in future versions.
    """,
)
annotation class ExperimentalDocumentStoreApi
