# OAuthError


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error** | **str** |  | 
**error_description** | **str** |  | [optional] 
**error_uri** | **str** |  | [optional] 

## Example

```python
from polaris.catalog.models.o_auth_error import OAuthError

# TODO update the JSON string below
json = "{}"
# create an instance of OAuthError from a JSON string
o_auth_error_instance = OAuthError.from_json(json)
# print the JSON string representation of the object
print(OAuthError.to_json())

# convert the object into a dict
o_auth_error_dict = o_auth_error_instance.to_dict()
# create an instance of OAuthError from a dict
o_auth_error_from_dict = OAuthError.from_dict(o_auth_error_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


