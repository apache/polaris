# EqualityDeleteFile


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**content** | **str** |  | 
**equality_ids** | **List[int]** | List of equality field IDs | [optional] 

## Example

```python
from polaris.catalog.models.equality_delete_file import EqualityDeleteFile

# TODO update the JSON string below
json = "{}"
# create an instance of EqualityDeleteFile from a JSON string
equality_delete_file_instance = EqualityDeleteFile.from_json(json)
# print the JSON string representation of the object
print(EqualityDeleteFile.to_json())

# convert the object into a dict
equality_delete_file_dict = equality_delete_file_instance.to_dict()
# create an instance of EqualityDeleteFile from a dict
equality_delete_file_from_dict = EqualityDeleteFile.from_dict(equality_delete_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


