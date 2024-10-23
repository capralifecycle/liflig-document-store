package no.liflig.documentstore.utils

import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Postgres stores timestamps with
 * [microsecond precision](https://www.postgresql.org/docs/17/datatype-datetime.html), while Java's
 * `Instant.now()` gives nanosecond precision when the operating system supports it. This caused
 * unpredictable results when comparing two `Versioned` entities, since we used `Instant.now()` for
 * the `createdAt`/`modifiedAt` fields. On systems that support nanosecond precision, a `Versioned`
 * entity returned by e.g. `Repository.create` may not compare as equal to the same entity when
 * later fetched out from the database, since the timestamp would be truncated to microsecond
 * precision by Postgres.
 *
 * To eliminate this unpredictability, we now use this function instead of `Instant.now()` when
 * getting the current time, with a maximum precision of microseconds.
 */
internal fun currentTimeWithMicrosecondPrecision(): Instant {
  return Instant.now().truncatedTo(ChronoUnit.MICROS)
}
