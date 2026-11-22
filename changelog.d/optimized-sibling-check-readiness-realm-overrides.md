### Fixes
- The production readiness check for `OPTIMIZED_SIBLING_CHECK` now evaluates each realm with its
  effective settings: a realm override where present, the default otherwise. Previously it looked
  at the override map alone, so a realm that enabled the check and inherited
  `ALLOW_OPTIMIZED_SIBLING_CHECK` from the defaults was refused at startup, while a realm that
  inherited the enabled check but overrode the acknowledgement to `false` was not reported.
