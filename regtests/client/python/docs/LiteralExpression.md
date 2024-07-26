# LiteralExpression


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**term** | [**Term**](Term.md) |  | 
**value** | **object** |  | 

## Example

```python
from polaris.catalog.models.literal_expression import LiteralExpression

# TODO update the JSON string below
json = "{}"
# create an instance of LiteralExpression from a JSON string
literal_expression_instance = LiteralExpression.from_json(json)
# print the JSON string representation of the object
print(LiteralExpression.to_json())

# convert the object into a dict
literal_expression_dict = literal_expression_instance.to_dict()
# create an instance of LiteralExpression from a dict
literal_expression_from_dict = LiteralExpression.from_dict(literal_expression_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


