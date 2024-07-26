# RemoveSnapshotsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**snapshot_ids** | **List[int]** |  | 

## Example

```python
from polaris.catalog.models.remove_snapshots_update import RemoveSnapshotsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemoveSnapshotsUpdate from a JSON string
remove_snapshots_update_instance = RemoveSnapshotsUpdate.from_json(json)
# print the JSON string representation of the object
print(RemoveSnapshotsUpdate.to_json())

# convert the object into a dict
remove_snapshots_update_dict = remove_snapshots_update_instance.to_dict()
# create an instance of RemoveSnapshotsUpdate from a dict
remove_snapshots_update_from_dict = RemoveSnapshotsUpdate.from_dict(remove_snapshots_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


