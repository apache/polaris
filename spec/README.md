# Polaris API Specifications
Polaris provides two sets of OpenAPI specifications:
- `polaris-management-service.yml` - Defines the management APIs for using Polaris to create and manage Iceberg catalogs and their principals
- `polaris-catalog-service.yaml` - Defines the specification for the Polaris Catalog API, which encompasses both the Iceberg REST Catalog API
   and Polaris-native API.
  - `polaris-apis` - Contains the specifications of Polaris-native API
  - `rest-catalog-open-api.yaml` - Contains the specification for Iceberg Rest Catalog API

## Generated Specification Files
The specification files in the generated folder are automatically created using OpenAPI bundling tools such as 
[Redocly CLI](https://github.com/Redocly/redocly-cli).

These files should not be manually edited (except adding license header). They are intended for preview purposes only, 
such as rendering a preview on a website.

Whenever the source specification files are updated, the generated files must be re-generated to reflect those changes.

Below are steps to generate `bundled-polaris-catalog-service.yaml`
### Install redocly-cli
```
npm install @redocly/cli -g
```

### Generate the Bundle
```
redocly bundle spec/polaris-catalog-service.yaml -o spec/generated/bundled-polaris-catalog-service.yaml
```


