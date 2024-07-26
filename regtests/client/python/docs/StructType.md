# StructType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**fields** | [**List[StructField]**](StructField.md) |  | 

## Example

```python
from polaris.catalog.models.struct_type import StructType

# TODO update the JSON string below
json = "{}"
# create an instance of StructType from a JSON string
struct_type_instance = StructType.from_json(json)
# print the JSON string representation of the object
print(StructType.to_json())

# convert the object into a dict
struct_type_dict = struct_type_instance.to_dict()
# create an instance of StructType from a dict
struct_type_from_dict = StructType.from_dict(struct_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


