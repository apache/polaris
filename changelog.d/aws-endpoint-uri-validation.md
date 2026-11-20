### New Features

- Added the `VALIDATE_AWS_ENDPOINT_URIS` feature flag (default: true). When enabled, the `endpoint`, `endpointInternal` and `stsEndpoint` of AWS storage configurations, and the SigV4 connection `stsEndpoint`, must be absolute http or https URIs with a host. This is checked on catalog create and update. Plain http endpoints log a warning on every validation.

### Changes

- `ALLOW_SETTING_S3_ENDPOINTS` is now checked on every catalog create and update, for the S3 endpoint properties of storage configurations and the SigV4 connection `stsEndpoint`. Previously, storage endpoints were checked only when an update included `storageConfigInfo`, and the SigV4 `stsEndpoint` only on create.

### Upgrade notes

- Before upgrading, check existing AWS catalogs for endpoint values that are not absolute http or https URIs. Updates to these catalogs are rejected while `VALIDATE_AWS_ENDPOINT_URIS` is true. Correct the values with a catalog update, or set the flag to false.
- With `ALLOW_SETTING_S3_ENDPOINTS` set to false, updates to catalogs that have S3 endpoint properties or a SigV4 `stsEndpoint` are rejected, even when the update doesn't touch them. Remove storage endpoints with a catalog update before setting the flag to false. Connection configuration can only be set on create, so recreate catalogs that have a SigV4 `stsEndpoint`, or keep the flag set to true.
