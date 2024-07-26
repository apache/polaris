# AddViewVersionUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**view_version** | [**ViewVersion**](ViewVersion.md) |  | 

## Example

```python
from polaris.catalog.models.add_view_version_update import AddViewVersionUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddViewVersionUpdate from a JSON string
add_view_version_update_instance = AddViewVersionUpdate.from_json(json)
# print the JSON string representation of the object
print(AddViewVersionUpdate.to_json())

# convert the object into a dict
add_view_version_update_dict = add_view_version_update_instance.to_dict()
# create an instance of AddViewVersionUpdate from a dict
add_view_version_update_from_dict = AddViewVersionUpdate.from_dict(add_view_version_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


