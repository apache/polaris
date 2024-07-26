# AddSchemaUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**last_column_id** | **int** | The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side. | [optional] 

## Example

```python
from polaris.catalog.models.add_schema_update import AddSchemaUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddSchemaUpdate from a JSON string
add_schema_update_instance = AddSchemaUpdate.from_json(json)
# print the JSON string representation of the object
print(AddSchemaUpdate.to_json())

# convert the object into a dict
add_schema_update_dict = add_schema_update_instance.to_dict()
# create an instance of AddSchemaUpdate from a dict
add_schema_update_from_dict = AddSchemaUpdate.from_dict(add_schema_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


