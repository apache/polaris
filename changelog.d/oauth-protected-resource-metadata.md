### New Features

- OAuth 2.0 Protected Resource Metadata (RFC 9728): when
  `quarkus.oidc.resource-metadata.enabled=true`, Polaris serves
  `/.well-known/oauth-protected-resource`, advertising the authorization server for the
  `/api/catalog` resource. This is server-side metadata only -- it benefits clients that
  already support RFC 9728 discovery; it does not change behavior for clients that don't.
