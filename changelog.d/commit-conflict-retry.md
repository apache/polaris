### Fixes

- Iceberg REST: table and view commits that lose the final compare-and-set to a concurrent commit
  are retried on the server again, up to `ICEBERG_COMMIT_MAX_RETRIES`, instead of returning
  `409 Conflict` immediately. The retry had stopped applying to this conflict since 1.1.0
  (see #5907). Commits that change a table location are also retried when sibling entities cannot
  be resolved for the location overlap check.
