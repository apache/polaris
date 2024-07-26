# SetDefaultSpecUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**spec_id** | **int** | Partition spec ID to set as the default, or -1 to set last added spec | 

## Example

```python
from polaris.catalog.models.set_default_spec_update import SetDefaultSpecUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetDefaultSpecUpdate from a JSON string
set_default_spec_update_instance = SetDefaultSpecUpdate.from_json(json)
# print the JSON string representation of the object
print(SetDefaultSpecUpdate.to_json())

# convert the object into a dict
set_default_spec_update_dict = set_default_spec_update_instance.to_dict()
# create an instance of SetDefaultSpecUpdate from a dict
set_default_spec_update_from_dict = SetDefaultSpecUpdate.from_dict(set_default_spec_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


