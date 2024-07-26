# SortOrder


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**order_id** | **int** |  | [readonly] 
**fields** | [**List[SortField]**](SortField.md) |  | 

## Example

```python
from polaris.catalog.models.sort_order import SortOrder

# TODO update the JSON string below
json = "{}"
# create an instance of SortOrder from a JSON string
sort_order_instance = SortOrder.from_json(json)
# print the JSON string representation of the object
print(SortOrder.to_json())

# convert the object into a dict
sort_order_dict = sort_order_instance.to_dict()
# create an instance of SortOrder from a dict
sort_order_from_dict = SortOrder.from_dict(sort_order_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


