# TimerResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**time_unit** | **str** |  | 
**count** | **int** |  | 
**total_duration** | **int** |  | 

## Example

```python
from polaris.catalog.models.timer_result import TimerResult

# TODO update the JSON string below
json = "{}"
# create an instance of TimerResult from a JSON string
timer_result_instance = TimerResult.from_json(json)
# print the JSON string representation of the object
print(TimerResult.to_json())

# convert the object into a dict
timer_result_dict = timer_result_instance.to_dict()
# create an instance of TimerResult from a dict
timer_result_from_dict = TimerResult.from_dict(timer_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


