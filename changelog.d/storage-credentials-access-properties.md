### Changes

- Include prefix-scoped storage settings in `storage-credentials` entries in table
  responses when credentials are vended. Top-level table configuration remains for
  compatibility. For clients applying scoped configuration, catalog-provided
  settings take precedence over client-configured values.
