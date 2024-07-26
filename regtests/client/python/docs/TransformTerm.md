# TransformTerm


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**transform** | **str** |  | 
**term** | **str** |  | 

## Example

```python
from polaris.catalog.models.transform_term import TransformTerm

# TODO update the JSON string below
json = "{}"
# create an instance of TransformTerm from a JSON string
transform_term_instance = TransformTerm.from_json(json)
# print the JSON string representation of the object
print(TransformTerm.to_json())

# convert the object into a dict
transform_term_dict = transform_term_instance.to_dict()
# create an instance of TransformTerm from a dict
transform_term_from_dict = TransformTerm.from_dict(transform_term_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


