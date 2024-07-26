# CatalogGrant


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**privilege** | [**CatalogPrivilege**](CatalogPrivilege.md) |  | 

## Example

```python
from polaris.management.models.catalog_grant import CatalogGrant

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogGrant from a JSON string
catalog_grant_instance = CatalogGrant.from_json(json)
# print the JSON string representation of the object
print(CatalogGrant.to_json())

# convert the object into a dict
catalog_grant_dict = catalog_grant_instance.to_dict()
# create an instance of CatalogGrant from a dict
catalog_grant_from_dict = CatalogGrant.from_dict(catalog_grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


