# AssertCurrentSchemaId

The table's current schema id must match the requirement's `current-schema-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**current_schema_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_current_schema_id import AssertCurrentSchemaId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertCurrentSchemaId from a JSON string
assert_current_schema_id_instance = AssertCurrentSchemaId.from_json(json)
# print the JSON string representation of the object
print(AssertCurrentSchemaId.to_json())

# convert the object into a dict
assert_current_schema_id_dict = assert_current_schema_id_instance.to_dict()
# create an instance of AssertCurrentSchemaId from a dict
assert_current_schema_id_from_dict = AssertCurrentSchemaId.from_dict(assert_current_schema_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


