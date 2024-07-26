# AddSnapshotUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**snapshot** | [**Snapshot**](Snapshot.md) |  | 

## Example

```python
from polaris.catalog.models.add_snapshot_update import AddSnapshotUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddSnapshotUpdate from a JSON string
add_snapshot_update_instance = AddSnapshotUpdate.from_json(json)
# print the JSON string representation of the object
print(AddSnapshotUpdate.to_json())

# convert the object into a dict
add_snapshot_update_dict = add_snapshot_update_instance.to_dict()
# create an instance of AddSnapshotUpdate from a dict
add_snapshot_update_from_dict = AddSnapshotUpdate.from_dict(add_snapshot_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


