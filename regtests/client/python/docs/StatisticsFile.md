# StatisticsFile


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**statistics_path** | **str** |  | 
**file_size_in_bytes** | **int** |  | 
**file_footer_size_in_bytes** | **int** |  | 
**blob_metadata** | [**List[BlobMetadata]**](BlobMetadata.md) |  | 

## Example

```python
from polaris.catalog.models.statistics_file import StatisticsFile

# TODO update the JSON string below
json = "{}"
# create an instance of StatisticsFile from a JSON string
statistics_file_instance = StatisticsFile.from_json(json)
# print the JSON string representation of the object
print(StatisticsFile.to_json())

# convert the object into a dict
statistics_file_dict = statistics_file_instance.to_dict()
# create an instance of StatisticsFile from a dict
statistics_file_from_dict = StatisticsFile.from_dict(statistics_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


