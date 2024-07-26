# SetStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**snapshot_id** | **int** |  | 
**statistics** | [**StatisticsFile**](StatisticsFile.md) |  | 

## Example

```python
from polaris.catalog.models.set_statistics_update import SetStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetStatisticsUpdate from a JSON string
set_statistics_update_instance = SetStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(SetStatisticsUpdate.to_json())

# convert the object into a dict
set_statistics_update_dict = set_statistics_update_instance.to_dict()
# create an instance of SetStatisticsUpdate from a dict
set_statistics_update_from_dict = SetStatisticsUpdate.from_dict(set_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


