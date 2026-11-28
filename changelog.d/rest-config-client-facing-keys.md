### Breaking changes

- Iceberg REST `GET /v1/config` no longer copies the catalog entity public property map into
  `defaults`. Clients that previously received `default-base-location`, `polaris.config.*`,
  `table-default.*`, leftover custom keys, catalog-level FileIO settings, or Iceberg REST keys
  stored on the catalog (`rest-page-size`, `rest-metrics-reporting-enabled`) from this endpoint
  will no longer see them there. Pagination stays on `LIST_PAGINATION_ENABLED` and
  `LIST_PAGINATION_MAX_PAGE_SIZE`. Other catalog properties remain on Management `GET` catalog.

### Upgrade notes

- Iceberg REST `GET /v1/config` `defaults` no longer copy catalog entity properties. `prefix` and
  `namespace-separator` remain in `overrides`. Catalog-level FileIO keys such as `s3.endpoint` are
  not advertised here; credential vending still sets them from storage configuration.
  `table-default.*` stays off this endpoint; Polaris still applies those defaults when a table is
  created.
