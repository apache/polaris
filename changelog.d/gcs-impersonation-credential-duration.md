### Changes

- GCS service account impersonation now requests the source token lifetime configured by
  `STORAGE_CREDENTIAL_DURATION_SECONDS`. The default remains one hour. If you configured a longer
  duration, the target service account must be allowed to request it by Google Cloud organization policy.
