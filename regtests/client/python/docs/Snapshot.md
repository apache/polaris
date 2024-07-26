# Snapshot


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**parent_snapshot_id** | **int** |  | [optional] 
**sequence_number** | **int** |  | [optional] 
**timestamp_ms** | **int** |  | 
**manifest_list** | **str** | Location of the snapshot&#39;s manifest list file | 
**summary** | [**SnapshotSummary**](SnapshotSummary.md) |  | 
**schema_id** | **int** |  | [optional] 

## Example

```python
from polaris.catalog.models.snapshot import Snapshot

# TODO update the JSON string below
json = "{}"
# create an instance of Snapshot from a JSON string
snapshot_instance = Snapshot.from_json(json)
# print the JSON string representation of the object
print(Snapshot.to_json())

# convert the object into a dict
snapshot_dict = snapshot_instance.to_dict()
# create an instance of Snapshot from a dict
snapshot_from_dict = Snapshot.from_dict(snapshot_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


