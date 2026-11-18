### Fixes

- Batch file cleanup now retries storage deletion failures reported by `FileIO`. Tasks that
  exhaust their retries remain persisted for replay, so persistent storage errors such as denied
  deletes can cause failed tasks to accumulate. Iceberg 1.12.0's Azure `ADLSFileIO` still logs
  and suppresses deletion exceptions ([#5482](https://github.com/apache/polaris/issues/5482)),
  and its `GCSFileIO` bulk deletion ignores per-object failure results.
