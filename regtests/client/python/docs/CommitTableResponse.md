# CommitTableResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata_location** | **str** |  | 
**metadata** | [**TableMetadata**](TableMetadata.md) |  | 

## Example

```python
from polaris.catalog.models.commit_table_response import CommitTableResponse

# TODO update the JSON string below
json = "{}"
# create an instance of CommitTableResponse from a JSON string
commit_table_response_instance = CommitTableResponse.from_json(json)
# print the JSON string representation of the object
print(CommitTableResponse.to_json())

# convert the object into a dict
commit_table_response_dict = commit_table_response_instance.to_dict()
# create an instance of CommitTableResponse from a dict
commit_table_response_from_dict = CommitTableResponse.from_dict(commit_table_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


