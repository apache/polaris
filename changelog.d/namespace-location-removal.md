### Fixes

- Removing a namespace's `location` property is now rejected with `400 Bad Request` when it would
  move the namespace. A namespace stored at a custom location fell back to its name-derived
  location and moved unchecked, including when `ALLOW_NAMESPACE_CUSTOM_LOCATION` is off and the
  stored location predates it. To move a namespace to its name-derived location, set `location` to
  that value first; removing the property from a namespace already there is unchanged.
