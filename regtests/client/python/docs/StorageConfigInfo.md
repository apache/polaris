# StorageConfigInfo

A storage configuration used by catalogs

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**storage_type** | **str** | The cloud provider type this storage is built on. FILE is supported for testing purposes only | 
**allowed_locations** | **List[str]** |  | [optional] 

## Example

```python
from polaris.management.models.storage_config_info import StorageConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of StorageConfigInfo from a JSON string
storage_config_info_instance = StorageConfigInfo.from_json(json)
# print the JSON string representation of the object
print(StorageConfigInfo.to_json())

# convert the object into a dict
storage_config_info_dict = storage_config_info_instance.to_dict()
# create an instance of StorageConfigInfo from a dict
storage_config_info_from_dict = StorageConfigInfo.from_dict(storage_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


