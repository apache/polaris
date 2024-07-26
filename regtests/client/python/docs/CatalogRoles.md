# CatalogRoles


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**roles** | [**List[CatalogRole]**](CatalogRole.md) | The list of catalog roles | 

## Example

```python
from polaris.management.models.catalog_roles import CatalogRoles

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogRoles from a JSON string
catalog_roles_instance = CatalogRoles.from_json(json)
# print the JSON string representation of the object
print(CatalogRoles.to_json())

# convert the object into a dict
catalog_roles_dict = catalog_roles_instance.to_dict()
# create an instance of CatalogRoles from a dict
catalog_roles_from_dict = CatalogRoles.from_dict(catalog_roles_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


