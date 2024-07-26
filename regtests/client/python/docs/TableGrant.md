# TableGrant


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** |  | 
**table_name** | **str** |  | 
**privilege** | [**TablePrivilege**](TablePrivilege.md) |  | 

## Example

```python
from polaris.management.models.table_grant import TableGrant

# TODO update the JSON string below
json = "{}"
# create an instance of TableGrant from a JSON string
table_grant_instance = TableGrant.from_json(json)
# print the JSON string representation of the object
print(TableGrant.to_json())

# convert the object into a dict
table_grant_dict = table_grant_instance.to_dict()
# create an instance of TableGrant from a dict
table_grant_from_dict = TableGrant.from_dict(table_grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


