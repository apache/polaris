### New Features

- The Iceberg REST `unregisterTable` endpoint removes an Iceberg table from a local
  catalog and returns its final metadata. All table files remain in storage. The
  endpoint is unavailable when `polaris.idempotency.enabled=true`.

### Changes

- Iceberg table drops with purge reject a concurrent table change with HTTP 503
  and `Retry-After: 3`. Clients can retry the drop with the current table state.
  A rejected drop does not schedule file cleanup.

### Upgrade notes

- Custom `BasePersistence` implementations used by `AtomicOperationMetaStoreManager`
  must implement `deleteEntityIfNotChanged` to support Iceberg table purges and
  unregister. A purge must remove the table entry atomically before it can schedule
  file cleanup.
- Custom `PolarisMetaStoreManager` implementations must implement
  `dropEntityIfNotChanged` to support unregister. The default implementation returns
  HTTP 406 (`Not Acceptable`) through the Iceberg REST API.
