# UpdateNamespacePropertiesRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**removals** | **List[str]** |  | [optional] 
**updates** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.update_namespace_properties_request import UpdateNamespacePropertiesRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateNamespacePropertiesRequest from a JSON string
update_namespace_properties_request_instance = UpdateNamespacePropertiesRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateNamespacePropertiesRequest.to_json())

# convert the object into a dict
update_namespace_properties_request_dict = update_namespace_properties_request_instance.to_dict()
# create an instance of UpdateNamespacePropertiesRequest from a dict
update_namespace_properties_request_from_dict = UpdateNamespacePropertiesRequest.from_dict(update_namespace_properties_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


