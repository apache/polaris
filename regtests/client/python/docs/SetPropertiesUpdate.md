# SetPropertiesUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**updates** | **Dict[str, str]** |  | 

## Example

```python
from polaris.catalog.models.set_properties_update import SetPropertiesUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetPropertiesUpdate from a JSON string
set_properties_update_instance = SetPropertiesUpdate.from_json(json)
# print the JSON string representation of the object
print(SetPropertiesUpdate.to_json())

# convert the object into a dict
set_properties_update_dict = set_properties_update_instance.to_dict()
# create an instance of SetPropertiesUpdate from a dict
set_properties_update_from_dict = SetPropertiesUpdate.from_dict(set_properties_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


