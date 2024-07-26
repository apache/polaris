# SnapshotSummary


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operation** | **str** |  | 

## Example

```python
from polaris.catalog.models.snapshot_summary import SnapshotSummary

# TODO update the JSON string below
json = "{}"
# create an instance of SnapshotSummary from a JSON string
snapshot_summary_instance = SnapshotSummary.from_json(json)
# print the JSON string representation of the object
print(SnapshotSummary.to_json())

# convert the object into a dict
snapshot_summary_dict = snapshot_summary_instance.to_dict()
# create an instance of SnapshotSummary from a dict
snapshot_summary_from_dict = SnapshotSummary.from_dict(snapshot_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


