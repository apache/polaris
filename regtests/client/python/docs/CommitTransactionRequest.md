# CommitTransactionRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_changes** | [**List[CommitTableRequest]**](CommitTableRequest.md) |  | 

## Example

```python
from polaris.catalog.models.commit_transaction_request import CommitTransactionRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CommitTransactionRequest from a JSON string
commit_transaction_request_instance = CommitTransactionRequest.from_json(json)
# print the JSON string representation of the object
print(CommitTransactionRequest.to_json())

# convert the object into a dict
commit_transaction_request_dict = commit_transaction_request_instance.to_dict()
# create an instance of CommitTransactionRequest from a dict
commit_transaction_request_from_dict = CommitTransactionRequest.from_dict(commit_transaction_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


