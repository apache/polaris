### Changes

- Creating a namespace, table, view or generic table now rejects a name used by a different entity
  kind under the same parent. Registering or renaming a table or view also checks its destination
  name for conflicts with namespaces. Concurrent operations can still race until these checks
  are enforced by the metastore.

### Upgrade notes

- Upgrading leaves existing sibling name conflicts in place. Rename or drop one of the conflicting
  entities to remove the conflict.
