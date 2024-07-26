# UpdateCatalogRequest

Updates to apply to a Catalog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**current_entity_version** | **int** | The version of the object onto which this update is applied; if the object changed, the update will fail and the caller should retry after fetching the latest version. | [optional] 
**properties** | **Dict[str, str]** |  | [optional] 
**storage_config_info** | [**StorageConfigInfo**](StorageConfigInfo.md) |  | [optional] 

## Example

```python
from polaris.management.models.update_catalog_request import UpdateCatalogRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateCatalogRequest from a JSON string
update_catalog_request_instance = UpdateCatalogRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateCatalogRequest.to_json())

# convert the object into a dict
update_catalog_request_dict = update_catalog_request_instance.to_dict()
# create an instance of UpdateCatalogRequest from a dict
update_catalog_request_from_dict = UpdateCatalogRequest.from_dict(update_catalog_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


