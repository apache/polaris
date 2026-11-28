### Fixes

- Batch file cleanup now retries storage deletion failures reported by `FileIO`. Tasks that
  exhaust their retries remain persisted for inspection and recovery using custom admin tools;
  Polaris does not automatically replay them. Persistent storage errors such as denied deletes
  can therefore cause failed tasks to accumulate. Iceberg 1.12.0's Azure `ADLSFileIO` still logs
  and suppresses deletion exceptions ([#5482](https://github.com/apache/polaris/issues/5482)),
  and its `GCSFileIO` bulk deletion ignores per-object failure results.
