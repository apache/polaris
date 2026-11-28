### New Features

- GCS vended credentials can now work on buckets with Hierarchical Namespace (HNS) enabled, with the
  same reach as on buckets without HNS. The write rule also covers the `folders/<path>/` prefix of
  each write location, so writers can create, rename and delete folders inside it, such as partition
  folders. Vended credentials get no access to folders outside the write locations. Instead, when
  the new realm-level feature flag `GCS_HNS_PARENT_FOLDER_CREATION_ENABLED` is `true` (it is `false`
  by default), Polaris creates the missing parent folders of each write location itself before
  vending write credentials, with the identity it downscopes from. HNS buckets need the flag:
  without it, writes fail until the parent folders exist. That identity must hold the
  `storage.folders.*` permissions on the bucket (for example through `roles/storage.objectAdmin`).
  Folder creation never blocks vending. Buckets without HNS reject it with 409, which is ignored, or
  with 403 when the identity lacks `storage.folders.create`; every failure other than 409 is logged
  as a warning. File cleanup tasks now request read, list and delete access instead of all access,
  which yields the same credentials on every storage type and creates no GCS folders. On buckets
  without HNS the vended credentials grant nothing extra.
