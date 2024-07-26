# StructField


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **int** |  | 
**name** | **str** |  | 
**type** | [**Type**](Type.md) |  | 
**required** | **bool** |  | 
**doc** | **str** |  | [optional] 

## Example

```python
from polaris.catalog.models.struct_field import StructField

# TODO update the JSON string below
json = "{}"
# create an instance of StructField from a JSON string
struct_field_instance = StructField.from_json(json)
# print the JSON string representation of the object
print(StructField.to_json())

# convert the object into a dict
struct_field_dict = struct_field_instance.to_dict()
# create an instance of StructField from a dict
struct_field_from_dict = StructField.from_dict(struct_field_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


