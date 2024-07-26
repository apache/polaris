# PartitionStatisticsFile


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**statistics_path** | **str** |  | 
**file_size_in_bytes** | **int** |  | 

## Example

```python
from polaris.catalog.models.partition_statistics_file import PartitionStatisticsFile

# TODO update the JSON string below
json = "{}"
# create an instance of PartitionStatisticsFile from a JSON string
partition_statistics_file_instance = PartitionStatisticsFile.from_json(json)
# print the JSON string representation of the object
print(PartitionStatisticsFile.to_json())

# convert the object into a dict
partition_statistics_file_dict = partition_statistics_file_instance.to_dict()
# create an instance of PartitionStatisticsFile from a dict
partition_statistics_file_from_dict = PartitionStatisticsFile.from_dict(partition_statistics_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


