# NamespaceGrant


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** |  | 
**privilege** | [**NamespacePrivilege**](NamespacePrivilege.md) |  | 

## Example

```python
from polaris.management.models.namespace_grant import NamespaceGrant

# TODO update the JSON string below
json = "{}"
# create an instance of NamespaceGrant from a JSON string
namespace_grant_instance = NamespaceGrant.from_json(json)
# print the JSON string representation of the object
print(NamespaceGrant.to_json())

# convert the object into a dict
namespace_grant_dict = namespace_grant_instance.to_dict()
# create an instance of NamespaceGrant from a dict
namespace_grant_from_dict = NamespaceGrant.from_dict(namespace_grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


