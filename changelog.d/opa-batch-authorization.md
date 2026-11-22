### New Features

- OPA authorizer: optional batch authorization that makes entity-level list filtering much faster.
  Without it, listing namespaces, tables or views for a caller who is filtered per entity costs one
  HTTP call to OPA for every candidate, so a list of 1,000 tables means 1,000 round trips. Set
  `polaris.authorization.opa.batch-policy-uri` to an OPA `batch_allow` rule and Polaris sends the
  candidates in chunks of `polaris.authorization.opa.max-batch-size` (default 100), so the same list
  takes 10 calls. It is off by default. When it is on, a failed or malformed batch response fails
  the request (HTTP 500) instead of denying quietly. See the OPA documentation for the rule and the
  input format.
