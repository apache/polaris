# MetadataLogInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata_file** | **str** |  | 
**timestamp_ms** | **int** |  | 

## Example

```python
from polaris.catalog.models.metadata_log_inner import MetadataLogInner

# TODO update the JSON string below
json = "{}"
# create an instance of MetadataLogInner from a JSON string
metadata_log_inner_instance = MetadataLogInner.from_json(json)
# print the JSON string representation of the object
print(MetadataLogInner.to_json())

# convert the object into a dict
metadata_log_inner_dict = metadata_log_inner_instance.to_dict()
# create an instance of MetadataLogInner from a dict
metadata_log_inner_from_dict = MetadataLogInner.from_dict(metadata_log_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


