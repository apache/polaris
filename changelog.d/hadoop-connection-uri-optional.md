### Fixes
- `uri` is now ignored for `HADOOP` connection configurations and is no longer validated. The
  Hadoop catalog is initialized from `warehouse` and never uses `uri`. Previously, values such as
  `hdfs://namenode:8020/warehouse` or `s3://bucket/warehouse` were rejected with
  `400 Invalid remote URI`, and omitting the field failed with `500 Internal Server Error`. A
  `HADOOP` catalog created without `uri` cannot be loaded by an older Polaris version, which matters
  only when downgrading.
- A missing `uri` on an `ICEBERG_REST` or `HIVE` connection now returns `400 Bad Request` with the
  message `uri is required for <TYPE> connections` instead of `500 Internal Server Error`.
