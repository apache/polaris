# SetCurrentViewVersionUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**view_version_id** | **int** | The view version id to set as current, or -1 to set last added view version id | 

## Example

```python
from polaris.catalog.models.set_current_view_version_update import SetCurrentViewVersionUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetCurrentViewVersionUpdate from a JSON string
set_current_view_version_update_instance = SetCurrentViewVersionUpdate.from_json(json)
# print the JSON string representation of the object
print(SetCurrentViewVersionUpdate.to_json())

# convert the object into a dict
set_current_view_version_update_dict = set_current_view_version_update_instance.to_dict()
# create an instance of SetCurrentViewVersionUpdate from a dict
set_current_view_version_update_from_dict = SetCurrentViewVersionUpdate.from_dict(set_current_view_version_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


