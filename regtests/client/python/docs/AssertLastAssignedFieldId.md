# AssertLastAssignedFieldId

The table's last assigned column id must match the requirement's `last-assigned-field-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**last_assigned_field_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_last_assigned_field_id import AssertLastAssignedFieldId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertLastAssignedFieldId from a JSON string
assert_last_assigned_field_id_instance = AssertLastAssignedFieldId.from_json(json)
# print the JSON string representation of the object
print(AssertLastAssignedFieldId.to_json())

# convert the object into a dict
assert_last_assigned_field_id_dict = assert_last_assigned_field_id_instance.to_dict()
# create an instance of AssertLastAssignedFieldId from a dict
assert_last_assigned_field_id_from_dict = AssertLastAssignedFieldId.from_dict(assert_last_assigned_field_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


