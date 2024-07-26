# ViewGrant


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** |  | 
**view_name** | **str** |  | 
**privilege** | [**ViewPrivilege**](ViewPrivilege.md) |  | 

## Example

```python
from polaris.management.models.view_grant import ViewGrant

# TODO update the JSON string below
json = "{}"
# create an instance of ViewGrant from a JSON string
view_grant_instance = ViewGrant.from_json(json)
# print the JSON string representation of the object
print(ViewGrant.to_json())

# convert the object into a dict
view_grant_dict = view_grant_instance.to_dict()
# create an instance of ViewGrant from a dict
view_grant_from_dict = ViewGrant.from_dict(view_grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


