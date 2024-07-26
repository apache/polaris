# ScanReport


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_name** | **str** |  | 
**snapshot_id** | **int** |  | 
**filter** | [**Expression**](Expression.md) |  | 
**schema_id** | **int** |  | 
**projected_field_ids** | **List[int]** |  | 
**projected_field_names** | **List[str]** |  | 
**metrics** | [**Dict[str, MetricResult]**](MetricResult.md) |  | 
**metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.scan_report import ScanReport

# TODO update the JSON string below
json = "{}"
# create an instance of ScanReport from a JSON string
scan_report_instance = ScanReport.from_json(json)
# print the JSON string representation of the object
print(ScanReport.to_json())

# convert the object into a dict
scan_report_dict = scan_report_instance.to_dict()
# create an instance of ScanReport from a dict
scan_report_from_dict = ScanReport.from_dict(scan_report_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


