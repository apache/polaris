# SetCurrentSchemaUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**schema_id** | **int** | Schema ID to set as current, or -1 to set last added schema | 

## Example

```python
from polaris.catalog.models.set_current_schema_update import SetCurrentSchemaUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetCurrentSchemaUpdate from a JSON string
set_current_schema_update_instance = SetCurrentSchemaUpdate.from_json(json)
# print the JSON string representation of the object
print(SetCurrentSchemaUpdate.to_json())

# convert the object into a dict
set_current_schema_update_dict = set_current_schema_update_instance.to_dict()
# create an instance of SetCurrentSchemaUpdate from a dict
set_current_schema_update_from_dict = SetCurrentSchemaUpdate.from_dict(set_current_schema_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


