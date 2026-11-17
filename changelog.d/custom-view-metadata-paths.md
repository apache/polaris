### Upgrade notes

- Views intentionally behave differently from tables for compatibility: commits that leave the
  base location and `write.metadata.path` unchanged enforce allowed locations but skip
  structured-location and overlap checks. This includes the first commit that records a legacy
  view's locations and continues while both paths remain unchanged. New views and changes to either
  path are validated under all applicable configured location rules.

### Breaking changes

- With `ALLOW_UNSTRUCTURED_TABLE_LOCATION=false` (default), creating a view or changing its
  location or `write.metadata.path` now rejects metadata paths outside the view's own location with
  403 Forbidden. Enable `ALLOW_UNSTRUCTURED_TABLE_LOCATION` to allow such paths within the catalog's
  allowed locations. Existing views can still commit updates that leave both paths unchanged;
  allowed-location restrictions continue to apply.
