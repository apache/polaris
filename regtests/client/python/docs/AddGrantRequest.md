# AddGrantRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**grant** | [**GrantResource**](GrantResource.md) |  | [optional] 

## Example

```python
from polaris.management.models.add_grant_request import AddGrantRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AddGrantRequest from a JSON string
add_grant_request_instance = AddGrantRequest.from_json(json)
# print the JSON string representation of the object
print(AddGrantRequest.to_json())

# convert the object into a dict
add_grant_request_dict = add_grant_request_instance.to_dict()
# create an instance of AddGrantRequest from a dict
add_grant_request_from_dict = AddGrantRequest.from_dict(add_grant_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


