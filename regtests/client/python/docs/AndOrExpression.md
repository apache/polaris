# AndOrExpression


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**left** | [**Expression**](Expression.md) |  | 
**right** | [**Expression**](Expression.md) |  | 

## Example

```python
from polaris.catalog.models.and_or_expression import AndOrExpression

# TODO update the JSON string below
json = "{}"
# create an instance of AndOrExpression from a JSON string
and_or_expression_instance = AndOrExpression.from_json(json)
# print the JSON string representation of the object
print(AndOrExpression.to_json())

# convert the object into a dict
and_or_expression_dict = and_or_expression_instance.to_dict()
# create an instance of AndOrExpression from a dict
and_or_expression_from_dict = AndOrExpression.from_dict(and_or_expression_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


