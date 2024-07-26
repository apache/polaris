# ViewHistoryEntry


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version_id** | **int** |  | 
**timestamp_ms** | **int** |  | 

## Example

```python
from polaris.catalog.models.view_history_entry import ViewHistoryEntry

# TODO update the JSON string below
json = "{}"
# create an instance of ViewHistoryEntry from a JSON string
view_history_entry_instance = ViewHistoryEntry.from_json(json)
# print the JSON string representation of the object
print(ViewHistoryEntry.to_json())

# convert the object into a dict
view_history_entry_dict = view_history_entry_instance.to_dict()
# create an instance of ViewHistoryEntry from a dict
view_history_entry_from_dict = ViewHistoryEntry.from_dict(view_history_entry_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


