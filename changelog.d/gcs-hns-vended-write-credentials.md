### New Features

- GCS vended credentials now work on buckets with Hierarchical Namespace (HNS) enabled, with the
  same reach as on buckets without HNS. The write rule also covers the `folders/<path>/` prefix of
  each write location, so writers can create, rename and delete folders inside it, such as partition
  folders. Vended credentials get no access to folders outside the write locations. Instead, before
  vending write credentials, Polaris creates the missing parent folders of each write location
  itself, with the identity it downscopes from. That identity must hold the `storage.folders.*`
  permissions on the bucket (for example through `roles/storage.objectAdmin`). Folder creation never
  blocks vending: buckets without HNS reject it and Polaris ignores that, and other failures are
  logged once per bucket. On buckets without HNS the vended credentials grant nothing extra.
