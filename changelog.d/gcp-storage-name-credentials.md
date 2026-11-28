### Upgrade notes

- When `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` is enabled, the `storageName` of a GCS storage configuration
  is no longer ignored. A GCS catalog whose `storageName` has no matching
  `polaris.storage.gcp.<storage>.credentials-file` now fails credential vending and storage access
  instead of using the default GCP credentials. A GCS catalog with a `storageName` also fails while
  `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true.

### New Features

- Added storage-scoped GCP credentials. With `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` enabled, a GCS storage
  configuration's `storageName` selects the credentials file set by
  `polaris.storage.gcp.<storage>.credentials-file`. Service account key files and Workload Identity
  Federation configuration files are accepted. A storage name is refused while
  `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true.
