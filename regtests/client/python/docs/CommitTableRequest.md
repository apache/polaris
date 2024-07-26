# CommitTableRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**identifier** | [**TableIdentifier**](TableIdentifier.md) |  | [optional] 
**requirements** | [**List[TableRequirement]**](TableRequirement.md) |  | 
**updates** | [**List[TableUpdate]**](TableUpdate.md) |  | 

## Example

```python
from polaris.catalog.models.commit_table_request import CommitTableRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CommitTableRequest from a JSON string
commit_table_request_instance = CommitTableRequest.from_json(json)
# print the JSON string representation of the object
print(CommitTableRequest.to_json())

# convert the object into a dict
commit_table_request_dict = commit_table_request_instance.to_dict()
# create an instance of CommitTableRequest from a dict
commit_table_request_from_dict = CommitTableRequest.from_dict(commit_table_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


