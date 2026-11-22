### Fixes

- NoSQL index stripe staging keys are injective: `IndexKey.toSafeString` now escapes `x` (the
  hex-escape prefix), so keys such as `"a b"` and `"ax20b"` no longer share a persist name.
  Previously two stripes in one commit could replace each other and the committed index would
  reference a stripe that was never written.
