# AssertTableUUID

The table UUID must match the requirement's `uuid`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**uuid** | **str** |  | 

## Example

```python
from polaris.catalog.models.assert_table_uuid import AssertTableUUID

# TODO update the JSON string below
json = "{}"
# create an instance of AssertTableUUID from a JSON string
assert_table_uuid_instance = AssertTableUUID.from_json(json)
# print the JSON string representation of the object
print(AssertTableUUID.to_json())

# convert the object into a dict
assert_table_uuid_dict = assert_table_uuid_instance.to_dict()
# create an instance of AssertTableUUID from a dict
assert_table_uuid_from_dict = AssertTableUUID.from_dict(assert_table_uuid_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


