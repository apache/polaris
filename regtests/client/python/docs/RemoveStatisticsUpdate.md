# RemoveStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**snapshot_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.remove_statistics_update import RemoveStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemoveStatisticsUpdate from a JSON string
remove_statistics_update_instance = RemoveStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(RemoveStatisticsUpdate.to_json())

# convert the object into a dict
remove_statistics_update_dict = remove_statistics_update_instance.to_dict()
# create an instance of RemoveStatisticsUpdate from a dict
remove_statistics_update_from_dict = RemoveStatisticsUpdate.from_dict(remove_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


