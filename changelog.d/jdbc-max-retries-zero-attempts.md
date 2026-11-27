### Fixes

- JDBC persistence now runs a database operation at least once when
  `polaris.persistence.relational.jdbc.max-retries` is set to `0`.
