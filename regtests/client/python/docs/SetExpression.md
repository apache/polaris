# SetExpression


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**term** | [**Term**](Term.md) |  | 
**values** | **List[object]** |  | 

## Example

```python
from polaris.catalog.models.set_expression import SetExpression

# TODO update the JSON string below
json = "{}"
# create an instance of SetExpression from a JSON string
set_expression_instance = SetExpression.from_json(json)
# print the JSON string representation of the object
print(SetExpression.to_json())

# convert the object into a dict
set_expression_dict = set_expression_instance.to_dict()
# create an instance of SetExpression from a dict
set_expression_from_dict = SetExpression.from_dict(set_expression_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


