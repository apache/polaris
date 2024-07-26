# AssertViewUUID

The view UUID must match the requirement's `uuid`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**uuid** | **str** |  | 

## Example

```python
from polaris.catalog.models.assert_view_uuid import AssertViewUUID

# TODO update the JSON string below
json = "{}"
# create an instance of AssertViewUUID from a JSON string
assert_view_uuid_instance = AssertViewUUID.from_json(json)
# print the JSON string representation of the object
print(AssertViewUUID.to_json())

# convert the object into a dict
assert_view_uuid_dict = assert_view_uuid_instance.to_dict()
# create an instance of AssertViewUUID from a dict
assert_view_uuid_from_dict = AssertViewUUID.from_dict(assert_view_uuid_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


