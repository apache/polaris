# AddSortOrderUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**sort_order** | [**SortOrder**](SortOrder.md) |  | 

## Example

```python
from polaris.catalog.models.add_sort_order_update import AddSortOrderUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddSortOrderUpdate from a JSON string
add_sort_order_update_instance = AddSortOrderUpdate.from_json(json)
# print the JSON string representation of the object
print(AddSortOrderUpdate.to_json())

# convert the object into a dict
add_sort_order_update_dict = add_sort_order_update_instance.to_dict()
# create an instance of AddSortOrderUpdate from a dict
add_sort_order_update_from_dict = AddSortOrderUpdate.from_dict(add_sort_order_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


