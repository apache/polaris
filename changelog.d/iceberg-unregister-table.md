### New Features

- The Iceberg REST `unregisterTable` endpoint removes an Iceberg table from a local
  catalog and returns its final metadata. All table files remain in storage. The
  endpoint is unavailable when `polaris.idempotency.enabled=true`.

### Upgrade notes

- Custom `BasePersistence` implementations used by `AtomicOperationMetaStoreManager`
  must implement `deleteEntityIfNotChanged` to support Iceberg table purges. A purge
  must remove the table entry atomically before it can schedule file cleanup.
