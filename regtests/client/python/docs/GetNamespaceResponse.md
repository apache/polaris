# GetNamespaceResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**properties** | **Dict[str, str]** | Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object. | [optional] 

## Example

```python
from polaris.catalog.models.get_namespace_response import GetNamespaceResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetNamespaceResponse from a JSON string
get_namespace_response_instance = GetNamespaceResponse.from_json(json)
# print the JSON string representation of the object
print(GetNamespaceResponse.to_json())

# convert the object into a dict
get_namespace_response_dict = get_namespace_response_instance.to_dict()
# create an instance of GetNamespaceResponse from a dict
get_namespace_response_from_dict = GetNamespaceResponse.from_dict(get_namespace_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


