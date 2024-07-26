# GcpStorageConfigInfo

gcp storage configuration info

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**gcs_service_account** | **str** | a Google cloud storage service account | [optional] 

## Example

```python
from polaris.management.models.gcp_storage_config_info import GcpStorageConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of GcpStorageConfigInfo from a JSON string
gcp_storage_config_info_instance = GcpStorageConfigInfo.from_json(json)
# print the JSON string representation of the object
print(GcpStorageConfigInfo.to_json())

# convert the object into a dict
gcp_storage_config_info_dict = gcp_storage_config_info_instance.to_dict()
# create an instance of GcpStorageConfigInfo from a dict
gcp_storage_config_info_from_dict = GcpStorageConfigInfo.from_dict(gcp_storage_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


