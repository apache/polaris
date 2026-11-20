### Fixes

- Dell ECS credential vending now uses `arn:aws:s3:::` for S3 session policy resources while
  preserving the ECS role URN in STS AssumeRole requests.
