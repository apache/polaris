### Highlights

- The authorizer can now be configured per realm, using the same pattern as the authenticator.
  Set `polaris.authorization.authorizer.type` as the default and override it for individual
  realms with `polaris.authorization.authorizer.<realm>.type`. A realm can also select a named
  authorizer configuration ("tenant") with `polaris.authorization.authorizer.<realm>.tenant-key`:
  OPA tenants are configured under `polaris.authorization.opa.<tenant-key>.*`, and Ranger tenants
  under `polaris.authorization.ranger.<tenant-key>.service-name`.

### Upgrade notes

- Ranger properties set directly under `polaris.authorization.ranger.*` are now only recognized
  with the `authz.`, `plugin.` and `xasecure.` prefixes, which cover all the properties read by the
  embedded Ranger authorizer. Polaris fails to start if other properties are set directly under
  `polaris.authorization.ranger.*` (except `service-name` and tenant configurations). Such
  properties were previously forwarded to the embedded Ranger authorizer, but Ranger does not read
  them; remove them, or move them under `polaris.authorization.ranger.properties.*`.

### Breaking changes

- The `PolarisAuthorizerFactory.create(RealmConfig)` method has been replaced with
  `create(@Nullable String)`, which receives the realm's authorizer tenant key (`null` when the
  realm uses the default authorizer configuration). Custom authorizer factories must implement the
  new method; those that need the `RealmConfig` can obtain it via CDI, as a request-scoped bean.

### Deprecations

- The `polaris.authorization.type` configuration property is deprecated in favor of
  `polaris.authorization.authorizer.type`. The legacy property is still honored when the new one is
  not set.
- Ranger properties set directly under `polaris.authorization.ranger.authz.*`,
  `polaris.authorization.ranger.plugin.*` and `polaris.authorization.ranger.xasecure.*` (e.g.
  `polaris.authorization.ranger.authz.default.policy.rest.url`) are deprecated in favor of
  `polaris.authorization.ranger.properties.*`. The legacy properties are still honored; when a
  property is set both ways, the new one takes precedence. Polaris logs a warning at startup when
  legacy properties are used.
