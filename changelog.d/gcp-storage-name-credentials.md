### Upgrade notes

- When `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` is enabled, the `storageName` of a GCS storage configuration
  is no longer ignored. A GCS catalog whose `storageName` has no matching
  `polaris.storage.gcp.<storage>.*` properties now fails credential vending and storage access instead
  of using the default GCP credentials. A GCS catalog with a `storageName` also fails while
  `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true.

### New Features

- Added storage-scoped GCP credentials. With `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` enabled, a GCS storage
  configuration's `storageName` selects the credentials file set by
  `polaris.storage.gcp.<storage>.credentials-file`, or the content of one set by
  `polaris.storage.gcp.<storage>.credentials`. Service account key files and Workload Identity
  Federation configuration files are accepted. The names apply to the whole deployment, not to one
  realm. A storage name is refused while `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true.
- Added `polaris.storage.gcp.credentials-file` and `polaris.storage.gcp.credentials` to set the GCP
  credentials used when no `storageName` selects named ones. When unset, `polaris.storage.gcp.token`
  or the application default credentials are used as before. They cannot be combined with
  `polaris.storage.gcp.token`.
