### New Features

- Added the `STORAGE_CREDENTIAL_REFRESH_BUFFER_SECONDS` feature flag (default `0`). When
  non-zero, a cached storage credential is evicted this many seconds before it actually
  expires, guaranteeing callers always receive credentials with at least this much validity
  left; a freshly loaded credential that is already within the buffer is reloaded once rather
  than handed back as-is, and if the reload is still under the buffer, an
  `UnprocessableEntityException` is thrown rather than returning a credential that violates
  the guarantee. Must be `>= 0` and less than `STORAGE_CREDENTIAL_DURATION_SECONDS`. When `0`
  (the default), behavior is unchanged: credentials are kept for half of their remaining
  lifetime.
