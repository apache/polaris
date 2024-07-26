# ValueMap


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**keys** | **List[int]** | List of integer column ids for each corresponding value | [optional] 
**values** | [**List[PrimitiveTypeValue]**](PrimitiveTypeValue.md) | List of primitive type values, matched to &#39;keys&#39; by index | [optional] 

## Example

```python
from polaris.catalog.models.value_map import ValueMap

# TODO update the JSON string below
json = "{}"
# create an instance of ValueMap from a JSON string
value_map_instance = ValueMap.from_json(json)
# print the JSON string representation of the object
print(ValueMap.to_json())

# convert the object into a dict
value_map_dict = value_map_instance.to_dict()
# create an instance of ValueMap from a dict
value_map_from_dict = ValueMap.from_dict(value_map_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


