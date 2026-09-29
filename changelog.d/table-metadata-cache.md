### New Features

- Table metadata JSON documents are now cached in memory by metadata file location, avoiding an
  object-storage read per table refresh. The approximate heap budget defaults to 5% of the maximum
  heap size, configurable via `polaris.table-metadata-cache.fraction-of-max-heap-size`, or can be
  fixed via `polaris.table-metadata-cache.max-bytes` (`0` in either disables caching). Documents
  larger than `polaris.table-metadata-cache.max-bytes-per-entry` (default 8 MiB, at most a tenth of
  the budget) are not cached, and documents not accessed for an hour expire.
