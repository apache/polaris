# SnapshotLogInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**timestamp_ms** | **int** |  | 

## Example

```python
from polaris.catalog.models.snapshot_log_inner import SnapshotLogInner

# TODO update the JSON string below
json = "{}"
# create an instance of SnapshotLogInner from a JSON string
snapshot_log_inner_instance = SnapshotLogInner.from_json(json)
# print the JSON string representation of the object
print(SnapshotLogInner.to_json())

# convert the object into a dict
snapshot_log_inner_dict = snapshot_log_inner_instance.to_dict()
# create an instance of SnapshotLogInner from a dict
snapshot_log_inner_from_dict = SnapshotLogInner.from_dict(snapshot_log_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


