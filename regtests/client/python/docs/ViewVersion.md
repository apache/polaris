# ViewVersion


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version_id** | **int** |  | 
**timestamp_ms** | **int** |  | 
**schema_id** | **int** | Schema ID to set as current, or -1 to set last added schema | 
**summary** | **Dict[str, str]** |  | 
**representations** | [**List[ViewRepresentation]**](ViewRepresentation.md) |  | 
**default_catalog** | **str** |  | [optional] 
**default_namespace** | **List[str]** | Reference to one or more levels of a namespace | 

## Example

```python
from polaris.catalog.models.view_version import ViewVersion

# TODO update the JSON string below
json = "{}"
# create an instance of ViewVersion from a JSON string
view_version_instance = ViewVersion.from_json(json)
# print the JSON string representation of the object
print(ViewVersion.to_json())

# convert the object into a dict
view_version_dict = view_version_instance.to_dict()
# create an instance of ViewVersion from a dict
view_version_from_dict = ViewVersion.from_dict(view_version_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


