# UpdateNamespacePropertiesResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**updated** | **List[str]** | List of property keys that were added or updated | 
**removed** | **List[str]** | List of properties that were removed | 
**missing** | **List[str]** | List of properties requested for removal that were not found in the namespace&#39;s properties. Represents a partial success response. Server&#39;s do not need to implement this. | [optional] 

## Example

```python
from polaris.catalog.models.update_namespace_properties_response import UpdateNamespacePropertiesResponse

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateNamespacePropertiesResponse from a JSON string
update_namespace_properties_response_instance = UpdateNamespacePropertiesResponse.from_json(json)
# print the JSON string representation of the object
print(UpdateNamespacePropertiesResponse.to_json())

# convert the object into a dict
update_namespace_properties_response_dict = update_namespace_properties_response_instance.to_dict()
# create an instance of UpdateNamespacePropertiesResponse from a dict
update_namespace_properties_response_from_dict = UpdateNamespacePropertiesResponse.from_dict(update_namespace_properties_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


