# AssertDefaultSpecId

The table's default spec id must match the requirement's `default-spec-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**default_spec_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_default_spec_id import AssertDefaultSpecId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertDefaultSpecId from a JSON string
assert_default_spec_id_instance = AssertDefaultSpecId.from_json(json)
# print the JSON string representation of the object
print(AssertDefaultSpecId.to_json())

# convert the object into a dict
assert_default_spec_id_dict = assert_default_spec_id_instance.to_dict()
# create an instance of AssertDefaultSpecId from a dict
assert_default_spec_id_from_dict = AssertDefaultSpecId.from_dict(assert_default_spec_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


