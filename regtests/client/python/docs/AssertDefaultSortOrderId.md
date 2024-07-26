# AssertDefaultSortOrderId

The table's default sort order id must match the requirement's `default-sort-order-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**default_sort_order_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_default_sort_order_id import AssertDefaultSortOrderId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertDefaultSortOrderId from a JSON string
assert_default_sort_order_id_instance = AssertDefaultSortOrderId.from_json(json)
# print the JSON string representation of the object
print(AssertDefaultSortOrderId.to_json())

# convert the object into a dict
assert_default_sort_order_id_dict = assert_default_sort_order_id_instance.to_dict()
# create an instance of AssertDefaultSortOrderId from a dict
assert_default_sort_order_id_from_dict = AssertDefaultSortOrderId.from_dict(assert_default_sort_order_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


