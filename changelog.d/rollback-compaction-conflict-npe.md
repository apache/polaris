### Fixes

- Failed table commits with `ICEBERG_ROLLBACK_COMPACTION_ON_CONFLICTS` enabled now return HTTP 409
  when the table has no current snapshot, or when a branch is created or removed concurrently.
