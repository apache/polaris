# ViewRepresentation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**sql** | **str** |  | 
**dialect** | **str** |  | 

## Example

```python
from polaris.catalog.models.view_representation import ViewRepresentation

# TODO update the JSON string below
json = "{}"
# create an instance of ViewRepresentation from a JSON string
view_representation_instance = ViewRepresentation.from_json(json)
# print the JSON string representation of the object
print(ViewRepresentation.to_json())

# convert the object into a dict
view_representation_dict = view_representation_instance.to_dict()
# create an instance of ViewRepresentation from a dict
view_representation_from_dict = ViewRepresentation.from_dict(view_representation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


