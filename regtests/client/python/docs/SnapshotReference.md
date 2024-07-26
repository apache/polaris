# SnapshotReference


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**snapshot_id** | **int** |  | 
**max_ref_age_ms** | **int** |  | [optional] 
**max_snapshot_age_ms** | **int** |  | [optional] 
**min_snapshots_to_keep** | **int** |  | [optional] 

## Example

```python
from polaris.catalog.models.snapshot_reference import SnapshotReference

# TODO update the JSON string below
json = "{}"
# create an instance of SnapshotReference from a JSON string
snapshot_reference_instance = SnapshotReference.from_json(json)
# print the JSON string representation of the object
print(SnapshotReference.to_json())

# convert the object into a dict
snapshot_reference_dict = snapshot_reference_instance.to_dict()
# create an instance of SnapshotReference from a dict
snapshot_reference_from_dict = SnapshotReference.from_dict(snapshot_reference_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


