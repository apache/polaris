# CounterResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**unit** | **str** |  | 
**value** | **int** |  | 

## Example

```python
from polaris.catalog.models.counter_result import CounterResult

# TODO update the JSON string below
json = "{}"
# create an instance of CounterResult from a JSON string
counter_result_instance = CounterResult.from_json(json)
# print the JSON string representation of the object
print(CounterResult.to_json())

# convert the object into a dict
counter_result_dict = counter_result_instance.to_dict()
# create an instance of CounterResult from a dict
counter_result_from_dict = CounterResult.from_dict(counter_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


