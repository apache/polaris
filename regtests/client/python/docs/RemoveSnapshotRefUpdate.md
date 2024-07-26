# RemoveSnapshotRefUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**ref_name** | **str** |  | 

## Example

```python
from polaris.catalog.models.remove_snapshot_ref_update import RemoveSnapshotRefUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemoveSnapshotRefUpdate from a JSON string
remove_snapshot_ref_update_instance = RemoveSnapshotRefUpdate.from_json(json)
# print the JSON string representation of the object
print(RemoveSnapshotRefUpdate.to_json())

# convert the object into a dict
remove_snapshot_ref_update_dict = remove_snapshot_ref_update_instance.to_dict()
# create an instance of RemoveSnapshotRefUpdate from a dict
remove_snapshot_ref_update_from_dict = RemoveSnapshotRefUpdate.from_dict(remove_snapshot_ref_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


