# CommitViewRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**identifier** | [**TableIdentifier**](TableIdentifier.md) |  | [optional] 
**requirements** | [**List[ViewRequirement]**](ViewRequirement.md) |  | [optional] 
**updates** | [**List[ViewUpdate]**](ViewUpdate.md) |  | 

## Example

```python
from polaris.catalog.models.commit_view_request import CommitViewRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CommitViewRequest from a JSON string
commit_view_request_instance = CommitViewRequest.from_json(json)
# print the JSON string representation of the object
print(CommitViewRequest.to_json())

# convert the object into a dict
commit_view_request_dict = commit_view_request_instance.to_dict()
# create an instance of CommitViewRequest from a dict
commit_view_request_from_dict = CommitViewRequest.from_dict(commit_view_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


