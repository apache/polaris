# CatalogProperties


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**default_base_location** | **str** |  | 

## Example

```python
from polaris.management.models.catalog_properties import CatalogProperties

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogProperties from a JSON string
catalog_properties_instance = CatalogProperties.from_json(json)
# print the JSON string representation of the object
print(CatalogProperties.to_json())

# convert the object into a dict
catalog_properties_dict = catalog_properties_instance.to_dict()
# create an instance of CatalogProperties from a dict
catalog_properties_from_dict = CatalogProperties.from_dict(catalog_properties_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


