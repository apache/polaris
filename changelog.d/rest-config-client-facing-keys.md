### Breaking changes

- Iceberg REST `GET /v1/config` no longer copies the catalog entity public property map into
  `defaults`. Clients that previously received `default-base-location`, `polaris.config.*`,
  `table-default.*`, leftover custom keys, or catalog-level FileIO settings from this endpoint
  will no longer see them there. Set `rest-page-size` or `rest-metrics-reporting-enabled` on the
  catalog when Iceberg clients should receive those values. Other catalog properties remain on
  Management `GET` catalog.

### Upgrade notes

- Iceberg REST `GET /v1/config` `defaults` now include only Iceberg REST client keys when they are
  set on the catalog: `rest-page-size` and `rest-metrics-reporting-enabled`. `prefix` and
  `namespace-separator` remain in `overrides`. Catalog entity properties such as
  `default-base-location`, `polaris.config.*`, registered catalog config keys (including legacy
  unprefixed names), `table-default.*`, leftover custom keys, and catalog-level FileIO keys are
  omitted from Iceberg defaults and remain available on Management `GET` catalog.
