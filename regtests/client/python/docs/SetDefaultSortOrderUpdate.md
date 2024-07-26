# SetDefaultSortOrderUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**sort_order_id** | **int** | Sort order ID to set as the default, or -1 to set last added sort order | 

## Example

```python
from polaris.catalog.models.set_default_sort_order_update import SetDefaultSortOrderUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetDefaultSortOrderUpdate from a JSON string
set_default_sort_order_update_instance = SetDefaultSortOrderUpdate.from_json(json)
# print the JSON string representation of the object
print(SetDefaultSortOrderUpdate.to_json())

# convert the object into a dict
set_default_sort_order_update_dict = set_default_sort_order_update_instance.to_dict()
# create an instance of SetDefaultSortOrderUpdate from a dict
set_default_sort_order_update_from_dict = SetDefaultSortOrderUpdate.from_dict(set_default_sort_order_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


