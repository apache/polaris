### New Features

- New `TABLE_READ_LINEAGE`, `TABLE_INGEST_LINEAGE`, `VIEW_READ_LINEAGE`, and `VIEW_INGEST_LINEAGE`
  privileges control access to lineage for tables and views. They can be granted to catalog roles
  at catalog and namespace scope, and on an individual table or view. Table and view lineage
  privileges never confer each other.

### Upgrade notes

- The Ranger service definition adds `table-lineage-query`, `table-lineage-ingest`,
  `view-lineage-query`, and `view-lineage-ingest` access types. Existing Ranger deployments must
  re-register the updated service definition with Ranger Admin to use them.
