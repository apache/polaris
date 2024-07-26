# TableIdentifier


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**name** | **str** |  | 

## Example

```python
from polaris.catalog.models.table_identifier import TableIdentifier

# TODO update the JSON string below
json = "{}"
# create an instance of TableIdentifier from a JSON string
table_identifier_instance = TableIdentifier.from_json(json)
# print the JSON string representation of the object
print(TableIdentifier.to_json())

# convert the object into a dict
table_identifier_dict = table_identifier_instance.to_dict()
# create an instance of TableIdentifier from a dict
table_identifier_from_dict = TableIdentifier.from_dict(table_identifier_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


