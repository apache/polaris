### Fixes

- Tokens issued by Polaris are now bound to the realm they were issued for, and are rejected when
  presented to a different realm.

### Upgrade notes

- Tokens issued by Polaris now carry a `polaris-realm` claim. Tokens issued by earlier versions
  remain valid as bearer tokens until they expire, but can no longer be used for token exchange;
  clients must request a new token using their client credentials.
