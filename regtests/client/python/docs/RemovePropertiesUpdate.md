# RemovePropertiesUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**removals** | **List[str]** |  | 

## Example

```python
from polaris.catalog.models.remove_properties_update import RemovePropertiesUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemovePropertiesUpdate from a JSON string
remove_properties_update_instance = RemovePropertiesUpdate.from_json(json)
# print the JSON string representation of the object
print(RemovePropertiesUpdate.to_json())

# convert the object into a dict
remove_properties_update_dict = remove_properties_update_instance.to_dict()
# create an instance of RemovePropertiesUpdate from a dict
remove_properties_update_from_dict = RemovePropertiesUpdate.from_dict(remove_properties_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


