### Fixes

- AWS credentials vended for write access now also allow `s3:AbortMultipartUpload` on the same
  locations as `s3:PutObject`, so clients can cancel failed multipart uploads. The vended policy is
  intersected with the catalog's IAM role, so the role must grant `s3:AbortMultipartUpload` as well
  for the change to take effect; roles that do not grant it keep working as before.
