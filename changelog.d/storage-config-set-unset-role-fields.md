### Fixes

- Updating a catalog now accepts an AWS account ID (set through the role ARN) or an external ID in
  the storage configuration when none was configured before, as the
  `ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES` description states. Changing an existing value
  is still rejected unless the flag is enabled.
