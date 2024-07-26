# SetLocationUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**location** | **str** |  | 

## Example

```python
from polaris.catalog.models.set_location_update import SetLocationUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetLocationUpdate from a JSON string
set_location_update_instance = SetLocationUpdate.from_json(json)
# print the JSON string representation of the object
print(SetLocationUpdate.to_json())

# convert the object into a dict
set_location_update_dict = set_location_update_instance.to_dict()
# create an instance of SetLocationUpdate from a dict
set_location_update_from_dict = SetLocationUpdate.from_dict(set_location_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


