# TableUpdateNotification


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_name** | **str** |  | 
**timestamp** | **int** |  | 
**table_uuid** | **str** |  | 
**metadata_location** | **str** |  | 
**metadata** | [**TableMetadata**](TableMetadata.md) |  | [optional] 

## Example

```python
from polaris.catalog.models.table_update_notification import TableUpdateNotification

# TODO update the JSON string below
json = "{}"
# create an instance of TableUpdateNotification from a JSON string
table_update_notification_instance = TableUpdateNotification.from_json(json)
# print the JSON string representation of the object
print(TableUpdateNotification.to_json())

# convert the object into a dict
table_update_notification_dict = table_update_notification_instance.to_dict()
# create an instance of TableUpdateNotification from a dict
table_update_notification_from_dict = TableUpdateNotification.from_dict(table_update_notification_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


