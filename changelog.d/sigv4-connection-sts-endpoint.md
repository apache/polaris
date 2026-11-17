### New Features

- Added optional `stsEndpoint` field to `SigV4AuthenticationParameters` for catalog federation. When set, Polaris calls AWS STS AssumeRole for the connection against this endpoint instead of the SDK default. The endpoint must be an absolute http or https URI, and plain http endpoints log a warning. This is allowed by default; operators can set `ALLOW_SETTING_S3_ENDPOINTS` to `false` to block it.
