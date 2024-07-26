# RemovePartitionStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**snapshot_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.remove_partition_statistics_update import RemovePartitionStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemovePartitionStatisticsUpdate from a JSON string
remove_partition_statistics_update_instance = RemovePartitionStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(RemovePartitionStatisticsUpdate.to_json())

# convert the object into a dict
remove_partition_statistics_update_dict = remove_partition_statistics_update_instance.to_dict()
# create an instance of RemovePartitionStatisticsUpdate from a dict
remove_partition_statistics_update_from_dict = RemovePartitionStatisticsUpdate.from_dict(remove_partition_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


