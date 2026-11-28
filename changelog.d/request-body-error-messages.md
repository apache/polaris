### Fixes
- A request body that cannot be read now gets a `400 Bad Request` whose Iceberg error envelope
  says what is wrong, on both the management and the Iceberg REST API. A body missing a required
  property or carrying a value of the wrong type previously returned `400` with an empty body, and
  malformed JSON returned the generic message `HTTP 400 Bad Request`; they now return messages such
  as `Invalid value: missing required property 'currentEntityVersion'`,
  `Invalid value: unknown type id 'NOPE' for property 'storageConfigInfo'` and
  `Invalid JSON: Unexpected end-of-input ...`. The messages name the property and the problem
  without exposing internal type names.
