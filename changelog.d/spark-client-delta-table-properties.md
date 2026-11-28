### Fixes

- Table properties set with `TBLPROPERTIES` when creating a Delta table through the Polaris Spark
  client are now persisted in Polaris and returned when the table is loaded. Previously they were
  dropped.
