# MapType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**key_id** | **int** |  | 
**key** | [**Type**](Type.md) |  | 
**value_id** | **int** |  | 
**value** | [**Type**](Type.md) |  | 
**value_required** | **bool** |  | 

## Example

```python
from polaris.catalog.models.map_type import MapType

# TODO update the JSON string below
json = "{}"
# create an instance of MapType from a JSON string
map_type_instance = MapType.from_json(json)
# print the JSON string representation of the object
print(MapType.to_json())

# convert the object into a dict
map_type_dict = map_type_instance.to_dict()
# create an instance of MapType from a dict
map_type_from_dict = MapType.from_dict(map_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


