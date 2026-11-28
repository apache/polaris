### Fixes

- Polaris's own S3 access no longer fetches AWS credentials from their source (IMDS, STS web
  identity, ...) again for each table operation when no credentials are configured, e.g. with
  `stsUnavailable: true`. Under load, this exceeded the IMDS rate limit and table operations failed.
