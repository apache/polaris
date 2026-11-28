### Fixes

- Creating an external catalog with a `HADOOP`, `HIVE` or `BIGQUERY` connection and an
  authentication type other than `IMPLICIT` now returns `400 Bad Request` when the federation
  extension for that connection type is installed (Hive and BigQuery Metastore federation are
  opt-in). These connection types only support `IMPLICIT` authentication; previously such a
  catalog was created, its secret was stored, and every later request against it failed with
  `500 Internal Server Error`. A catalog that was created with such a combination before this
  change cannot serve any request and is now also rejected when updated; drop and recreate it
  with `IMPLICIT` authentication.
