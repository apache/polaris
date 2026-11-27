### New Features

- Opt-in Iceberg table soft-delete (default off): when `TABLE_SOFT_DELETE_ENABLED` is true, `DROP
  TABLE` without purge keeps catalog metadata for a hold period (`TABLE_SOFT_DELETE_HOLD_PERIOD`,
  default `P7D`), hides the table from Iceberg REST list/load/HEAD, and reserves the identifier
  until permanent delete. `DROP TABLE PURGE` remains an immediate permanent delete. After the hold,
  run the Admin Tool `expire-soft-deleted-tables` command to permanently remove catalog state; file
  cleanup is opt-in via `TABLE_SOFT_DELETE_PURGE_DATA_ON_PERMANENT_DELETE`. INTERNAL catalogs only.
