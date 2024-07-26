# AssertCreate

The table must not already exist; used for create transactions

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 

## Example

```python
from polaris.catalog.models.assert_create import AssertCreate

# TODO update the JSON string below
json = "{}"
# create an instance of AssertCreate from a JSON string
assert_create_instance = AssertCreate.from_json(json)
# print the JSON string representation of the object
print(AssertCreate.to_json())

# convert the object into a dict
assert_create_dict = assert_create_instance.to_dict()
# create an instance of AssertCreate from a dict
assert_create_from_dict = AssertCreate.from_dict(assert_create_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


