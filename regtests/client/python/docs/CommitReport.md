# CommitReport


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_name** | **str** |  | 
**snapshot_id** | **int** |  | 
**sequence_number** | **int** |  | 
**operation** | **str** |  | 
**metrics** | [**Dict[str, MetricResult]**](MetricResult.md) |  | 
**metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.commit_report import CommitReport

# TODO update the JSON string below
json = "{}"
# create an instance of CommitReport from a JSON string
commit_report_instance = CommitReport.from_json(json)
# print the JSON string representation of the object
print(CommitReport.to_json())

# convert the object into a dict
commit_report_dict = commit_report_instance.to_dict()
# create an instance of CommitReport from a dict
commit_report_from_dict = CommitReport.from_dict(commit_report_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


