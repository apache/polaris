# RenameTableRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source** | [**TableIdentifier**](TableIdentifier.md) |  | 
**destination** | [**TableIdentifier**](TableIdentifier.md) |  | 

## Example

```python
from polaris.catalog.models.rename_table_request import RenameTableRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RenameTableRequest from a JSON string
rename_table_request_instance = RenameTableRequest.from_json(json)
# print the JSON string representation of the object
print(RenameTableRequest.to_json())

# convert the object into a dict
rename_table_request_dict = rename_table_request_instance.to_dict()
# create an instance of RenameTableRequest from a dict
rename_table_request_from_dict = RenameTableRequest.from_dict(rename_table_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


