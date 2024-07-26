# SQLViewRepresentation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**sql** | **str** |  | 
**dialect** | **str** |  | 

## Example

```python
from polaris.catalog.models.sql_view_representation import SQLViewRepresentation

# TODO update the JSON string below
json = "{}"
# create an instance of SQLViewRepresentation from a JSON string
sql_view_representation_instance = SQLViewRepresentation.from_json(json)
# print the JSON string representation of the object
print(SQLViewRepresentation.to_json())

# convert the object into a dict
sql_view_representation_dict = sql_view_representation_instance.to_dict()
# create an instance of SQLViewRepresentation from a dict
sql_view_representation_from_dict = SQLViewRepresentation.from_dict(sql_view_representation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


