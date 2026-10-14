### Fixes

- Registering an Iceberg table with `overwrite=true` now preserves its idempotency keys when
  the table UUID is unchanged. Within-TTL create and update retries continue to replay success,
  preventing an older update from overwriting newer table properties. Replacements with a
  different UUID do not inherit the previous table's keys.
