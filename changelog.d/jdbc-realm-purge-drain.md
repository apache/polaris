### Upgrade notes

- Relational JDBC: the migration to schema v7 is **required** before starting this version of
  Polaris. It adds the `realm_purge` table, which records realms whose time-series data is still
  pending deletion. The table is a pure addition, so it can be created while the previous version is
  still serving, which keeps the restart the only window where no node can serve. Time-series data
  of realms purged by an earlier version is not queued and is not reclaimed by the drain unless the
  optional one-off statement in the migration is run. See the [Relational JDBC metastore
  documentation] for the migration SQL and the full upgrade path.

### New Features

- New admin tool command `purge-drain`: deletes the time-series data of already purged realms in
  bounded batches. Intended to be scheduled periodically, like `nosql maintenance-run`.

### Fixes

- Relational JDBC: purging a realm no longer leaves its events and metrics reports behind. The
  realm is queued for an asynchronous, batched deletion of that data, drained by the new
  `purge-drain` admin tool command.
