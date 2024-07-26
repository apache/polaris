# SetPartitionStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**partition_statistics** | [**PartitionStatisticsFile**](PartitionStatisticsFile.md) |  | 

## Example

```python
from polaris.catalog.models.set_partition_statistics_update import SetPartitionStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetPartitionStatisticsUpdate from a JSON string
set_partition_statistics_update_instance = SetPartitionStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(SetPartitionStatisticsUpdate.to_json())

# convert the object into a dict
set_partition_statistics_update_dict = set_partition_statistics_update_instance.to_dict()
# create an instance of SetPartitionStatisticsUpdate from a dict
set_partition_statistics_update_from_dict = SetPartitionStatisticsUpdate.from_dict(set_partition_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


