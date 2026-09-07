### Fixes

- Table and view metadata updates now reconcile unknown persistence outcomes by reading back the
  metadata location and notification timestamp. Confirmed updates return success; unconfirmed
  updates retain their metadata files and report an unknown outcome.
