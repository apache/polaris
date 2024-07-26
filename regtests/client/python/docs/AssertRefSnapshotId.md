# AssertRefSnapshotId

The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**ref** | **str** |  | 
**snapshot_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_ref_snapshot_id import AssertRefSnapshotId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertRefSnapshotId from a JSON string
assert_ref_snapshot_id_instance = AssertRefSnapshotId.from_json(json)
# print the JSON string representation of the object
print(AssertRefSnapshotId.to_json())

# convert the object into a dict
assert_ref_snapshot_id_dict = assert_ref_snapshot_id_instance.to_dict()
# create an instance of AssertRefSnapshotId from a dict
assert_ref_snapshot_id_from_dict = AssertRefSnapshotId.from_dict(assert_ref_snapshot_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


