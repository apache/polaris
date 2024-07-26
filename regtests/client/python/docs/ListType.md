# ListType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**element_id** | **int** |  | 
**element** | [**Type**](Type.md) |  | 
**element_required** | **bool** |  | 

## Example

```python
from polaris.catalog.models.list_type import ListType

# TODO update the JSON string below
json = "{}"
# create an instance of ListType from a JSON string
list_type_instance = ListType.from_json(json)
# print the JSON string representation of the object
print(ListType.to_json())

# convert the object into a dict
list_type_dict = list_type_instance.to_dict()
# create an instance of ListType from a dict
list_type_from_dict = ListType.from_dict(list_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


