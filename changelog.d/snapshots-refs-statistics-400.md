### Fixes

- Loading a table with `snapshots=refs` no longer fails with HTTP 400 (`Cannot set metadata location
  with changes to table metadata`) when a snapshot outside the table's refs has statistics or
  partition statistics files, which Trino writes on every insert by default.
