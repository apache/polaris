# SortField


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_id** | **int** |  | 
**transform** | **str** |  | 
**direction** | [**SortDirection**](SortDirection.md) |  | 
**null_order** | [**NullOrder**](NullOrder.md) |  | 

## Example

```python
from polaris.catalog.models.sort_field import SortField

# TODO update the JSON string below
json = "{}"
# create an instance of SortField from a JSON string
sort_field_instance = SortField.from_json(json)
# print the JSON string representation of the object
print(SortField.to_json())

# convert the object into a dict
sort_field_dict = sort_field_instance.to_dict()
# create an instance of SortField from a dict
sort_field_from_dict = SortField.from_dict(sort_field_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


