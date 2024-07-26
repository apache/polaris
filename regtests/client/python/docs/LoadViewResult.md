# LoadViewResult

Result used when a view is successfully loaded.   The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata is returned in the `metadata-location` field. Clients can check whether metadata has changed by comparing metadata locations after the view has been created.  The `config` map returns view-specific configuration for the view's resources.  The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata_location** | **str** |  | 
**metadata** | [**ViewMetadata**](ViewMetadata.md) |  | 
**config** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.load_view_result import LoadViewResult

# TODO update the JSON string below
json = "{}"
# create an instance of LoadViewResult from a JSON string
load_view_result_instance = LoadViewResult.from_json(json)
# print the JSON string representation of the object
print(LoadViewResult.to_json())

# convert the object into a dict
load_view_result_dict = load_view_result_instance.to_dict()
# create an instance of LoadViewResult from a dict
load_view_result_from_dict = LoadViewResult.from_dict(load_view_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


