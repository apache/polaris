### New Features

- Added a Table Metrics Reports REST API (beta) for querying persisted Iceberg scan and commit metrics reports. The OpenAPI spec and query SPI ship in the `polaris-extensions-metrics-reports*` extension modules; the HTTP endpoint returns an empty result (HTTP 200) unless a durable query backend (e.g. the JDBC extension) is installed. Report payloads are reconstructed from the persisted metrics records and contain only the persisted fields, not the complete original Iceberg report. Reads are gated by the new `TABLE_READ_METRICS` privilege.
