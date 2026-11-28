### Upgrade notes

- When `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` is enabled, the `storageName` of an Azure storage
  configuration is no longer ignored. An Azure catalog whose `storageName` has no matching
  `polaris.storage.azure.<storage>.*` properties now fails credential vending and storage access
  instead of using the default Azure credentials.

### New Features

- Added storage-scoped Azure credentials. With `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` enabled, an Azure
  storage configuration's `storageName` selects the service principal set by
  `polaris.storage.azure.<storage>.tenant-id`, `client-id` and `client-secret`. The storage
  configuration's `tenantId` must match the configured tenant.
- Added `polaris.storage.azure.tenant-id`, `client-id` and `client-secret` to set the Azure service
  principal used when no `storageName` selects a named one, like `polaris.storage.aws.access-key` and
  `secret-key` for AWS. When unset, the `DefaultAzureCredential` chain is used as before.
