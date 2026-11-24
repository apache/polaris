### Fixes

- Creating a table named after an Iceberg metadata table type, such as `history`, `snapshots` or
  `entries`, no longer fails with an `IllegalStateException` (HTTP 500).
