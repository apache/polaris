### Upgrade notes

- Catalog federation: `BIGQUERY` connection properties now follow the same allow-list as
  `HADOOP`, `HIVE`, and `ICEBERG_REST`. Only the documented BigQuery client keys
  (`gcp.bigquery.project-id`, `warehouse`, `gcp.bigquery.location`,
  `gcp.bigquery.list-all-tables`, and the `gcp.bigquery.impersonate.*` keys) are forwarded to
  the Iceberg client. Other keys, including `io-impl` and credential keys, are stored but ignored.
  Move `io-impl` into catalog-level `properties` if it was set on the connection.
