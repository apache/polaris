# AssertLastAssignedPartitionId

The table's last assigned partition id must match the requirement's `last-assigned-partition-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**last_assigned_partition_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_last_assigned_partition_id import AssertLastAssignedPartitionId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertLastAssignedPartitionId from a JSON string
assert_last_assigned_partition_id_instance = AssertLastAssignedPartitionId.from_json(json)
# print the JSON string representation of the object
print(AssertLastAssignedPartitionId.to_json())

# convert the object into a dict
assert_last_assigned_partition_id_dict = assert_last_assigned_partition_id_instance.to_dict()
# create an instance of AssertLastAssignedPartitionId from a dict
assert_last_assigned_partition_id_from_dict = AssertLastAssignedPartitionId.from_dict(assert_last_assigned_partition_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


