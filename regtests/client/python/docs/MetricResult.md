# MetricResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**unit** | **str** |  | 
**value** | **int** |  | 
**time_unit** | **str** |  | 
**count** | **int** |  | 
**total_duration** | **int** |  | 

## Example

```python
from polaris.catalog.models.metric_result import MetricResult

# TODO update the JSON string below
json = "{}"
# create an instance of MetricResult from a JSON string
metric_result_instance = MetricResult.from_json(json)
# print the JSON string representation of the object
print(MetricResult.to_json())

# convert the object into a dict
metric_result_dict = metric_result_instance.to_dict()
# create an instance of MetricResult from a dict
metric_result_from_dict = MetricResult.from_dict(metric_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


