# CreateNamespaceResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**properties** | **Dict[str, str]** | Properties stored on the namespace, if supported by the server. | [optional] 

## Example

```python
from polaris.catalog.models.create_namespace_response import CreateNamespaceResponse

# TODO update the JSON string below
json = "{}"
# create an instance of CreateNamespaceResponse from a JSON string
create_namespace_response_instance = CreateNamespaceResponse.from_json(json)
# print the JSON string representation of the object
print(CreateNamespaceResponse.to_json())

# convert the object into a dict
create_namespace_response_dict = create_namespace_response_instance.to_dict()
# create an instance of CreateNamespaceResponse from a dict
create_namespace_response_from_dict = CreateNamespaceResponse.from_dict(create_namespace_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


