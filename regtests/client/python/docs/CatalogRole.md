# CatalogRole


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | The name of the role | 
**properties** | **Dict[str, str]** |  | [optional] 
**create_timestamp** | **int** |  | [optional] 
**last_update_timestamp** | **int** |  | [optional] 
**entity_version** | **int** | The version of the catalog role object used to determine if the catalog role metadata has changed | [optional] 

## Example

```python
from polaris.management.models.catalog_role import CatalogRole

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogRole from a JSON string
catalog_role_instance = CatalogRole.from_json(json)
# print the JSON string representation of the object
print(CatalogRole.to_json())

# convert the object into a dict
catalog_role_dict = catalog_role_instance.to_dict()
# create an instance of CatalogRole from a dict
catalog_role_from_dict = CatalogRole.from_dict(catalog_role_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


