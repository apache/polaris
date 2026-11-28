### Fixes

- Updating a catalog without `properties`, for example to change only its `storageConfigInfo`, no
  longer removes all of its properties except `default-base-location`. A `properties` map in the
  request still replaces the catalog's properties, and an empty map still clears them.

### Upgrade notes

- In `polaris-api-management-model`, `UpdateCatalogRequest.getProperties()` now returns `null`
  instead of an empty map when the request omits `properties`.
