### New Features

- SigV4 catalog federation can now sign with static keys from a named server-side AWS credential,
  referenced by `storageName` in `SigV4AuthenticationParameters`. When `roleArn` is also set, those
  keys call STS AssumeRole. Otherwise they sign directly, which covers Iceberg REST catalogs that
  have no IAM role, and AWS services such as Glue when the operator prefers named keys. Operators
  configure the keys under `polaris.storage.aws.<storage-name>.access-key` / `.secret-key` and
  enable `RESOLVE_CREDENTIALS_BY_STORAGE_NAME`. At least one of `roleArn` or `storageName` must be
  set. `roleSessionName` / `externalId` / `sessionPolicy` require `roleArn`.
