### Fixes

- Tokens issued by Polaris are now bound to the realm they were issued for, and are rejected when
  presented to a different realm.

### Upgrade notes

- Tokens issued by Polaris now carry a `polaris-realm` claim. Tokens issued by earlier versions do
  not, so they are not bound to a realm: they remain valid as bearer tokens until they expire, but
  can no longer be used for token exchange; clients must request a new token using their client
  credentials. During a rolling upgrade, nodes that have not been upgraded yet do not check the
  claim either. Realm binding is therefore only fully effective once all nodes have been upgraded
  and tokens issued by earlier versions have expired (after `max-token-generation`, one hour by
  default). Deployments that cannot wait can configure a distinct token signing key for each realm,
  which invalidates tokens issued with the previous key.
