### New Features

- The Iceberg REST `unregisterTable` endpoint removes an Iceberg table from a local
  catalog and returns its final metadata. All table files remain in storage. The
  endpoint is unavailable when `polaris.idempotency.enabled=true`.
