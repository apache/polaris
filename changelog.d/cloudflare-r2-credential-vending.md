### Upgrade notes

- Cloudflare R2: a node without the `polaris-extensions-storage-cloudflare-r2` extension cannot
  vend credentials for an `R2_LOCAL_SIGNING` catalog, so create such catalogs only after every
  node runs a build that includes the extension. A node without the extension also fails to start
  when a `polaris.storage.cloudflare-r2.*` system property is set (`SRCFG00050`), so add those
  properties only to nodes that run such a build.

### New Features

- Cloudflare R2 credential vending. The new extension module
  `polaris-extensions-storage-cloudflare-r2` provides the `R2_LOCAL_SIGNING` credential vending
  mechanism for S3-compatible storage that uses an R2 endpoint. The server signs temporary R2
  credentials locally from a parent token in its configuration, scoped to the table's location. Set
  the default parent token with `polaris.storage.cloudflare-r2.access-key` and `.secret-key`, and
  named tokens with `polaris.storage.cloudflare-r2.<storage>.access-key` and `.secret-key`. Named
  tokens apply only when `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` is true. A realm opts in by adding
  `R2_LOCAL_SIGNING` to `SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS`, whose default stays `[STS]`.
  The parent tokens are server-wide: every realm that opts in can use them. Anyone with
  `CATALOG_CREATE` or `CATALOG_WRITE_PROPERTIES` chooses a catalog's buckets within the reach of
  its parent token, and its `storageName` when named tokens apply, so each parent token should
  cover only the buckets its catalogs need. The server bundle includes the module.
