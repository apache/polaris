# NotExpression


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**child** | [**Expression**](Expression.md) |  | 

## Example

```python
from polaris.catalog.models.not_expression import NotExpression

# TODO update the JSON string below
json = "{}"
# create an instance of NotExpression from a JSON string
not_expression_instance = NotExpression.from_json(json)
# print the JSON string representation of the object
print(NotExpression.to_json())

# convert the object into a dict
not_expression_dict = not_expression_instance.to_dict()
# create an instance of NotExpression from a dict
not_expression_from_dict = NotExpression.from_dict(not_expression_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


