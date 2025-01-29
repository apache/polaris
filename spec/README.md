# Polaris API Specifications
Polaris provides two sets of OpenAPI specifications:
- `polaris-management-service.yml` - Defines the management APIs for using Polaris to create and manage Iceberg catalogs and their principals
- `polaris-catalog-service.yaml` - Defines the specification for the Polaris Catalog API, which encompasses both the Iceberg REST Catalog API
   and Polaris-native API.
  - `polaris-apis` - Contains the specification of Polaris-native API
  - `iceberg-rest-api` - Contains the specification for Iceberg Rest Catalog API [1]

Note:

1. The `rest-catalog-open-api.yaml` in `iceberg-rest-api` should match a released version of Iceberg Rest Catalog OpenAPI spec and should not be changed manually



