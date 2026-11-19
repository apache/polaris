### Breaking changes

- `PolarisMetaStoreManager` has a new abstract method
  `writeEntitiesIfConditionsHold(PolarisCallContext, ConditionalEntityWrite)`, which applies entity
  writes atomically if a set of conditions on other entities holds, and `BasePersistence` has a new
  abstract method `checkAndWriteEntities(PolarisCallContext, List<Condition>, List<EntityChange>)`.
  The method `PolarisMetaStoreManager.updateEntitiesPropertiesIfNotChanged` is now a default method
  that delegates to `writeEntitiesIfConditionsHold`. Custom implementations of these interfaces must
  implement the new methods. Conditions, writes and entity changes are sealed types: new kinds are
  added as new members rather than new methods, and a backend must handle each kind.

### Deprecations

- `PolarisMetaStoreManager.updateEntitiesPropertiesIfNotChanged` is deprecated in favor of
  `writeEntitiesIfConditionsHold`, which it delegates to.

### Fixes

- A multi-table commit (`POST /v1/{prefix}/transactions/commit`) now enforces the requirements of
  tables it does not update (`updates: []`) when the transaction commits. Previously two concurrent
  commits that each changed one table and asserted that the other was unchanged could both succeed
  (write skew), because such tables did not take part in the atomic commit. Such a table is now
  checked, but not written, together with the updates of the other tables, and the commit fails
  with a retryable conflict if it was modified in the meantime. Transactions that only assert the
  same table do not fail each other. On PostgreSQL, two multi-table commits that update the
  same tables in a different order now fail with a retryable conflict instead of a database deadlock
  that surfaced as an internal server error.
