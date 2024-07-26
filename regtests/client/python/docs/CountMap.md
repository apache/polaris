# CountMap


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**keys** | **List[int]** | List of integer column ids for each corresponding value | [optional] 
**values** | **List[int]** | List of Long values, matched to &#39;keys&#39; by index | [optional] 

## Example

```python
from polaris.catalog.models.count_map import CountMap

# TODO update the JSON string below
json = "{}"
# create an instance of CountMap from a JSON string
count_map_instance = CountMap.from_json(json)
# print the JSON string representation of the object
print(CountMap.to_json())

# convert the object into a dict
count_map_dict = count_map_instance.to_dict()
# create an instance of CountMap from a dict
count_map_from_dict = CountMap.from_dict(count_map_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


