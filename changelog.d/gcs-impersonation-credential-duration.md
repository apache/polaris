### Changes

- GCS service account impersonation now uses `STORAGE_CREDENTIAL_DURATION_SECONDS`, the same
  realm setting that S3 and Azure use. Set it below 3600 to request a shorter GCS source token.
  At 3600 or above, Polaris continues to request one hour from Google IAM.
