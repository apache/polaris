# AddPartitionSpecUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**spec** | [**PartitionSpec**](PartitionSpec.md) |  | 

## Example

```python
from polaris.catalog.models.add_partition_spec_update import AddPartitionSpecUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddPartitionSpecUpdate from a JSON string
add_partition_spec_update_instance = AddPartitionSpecUpdate.from_json(json)
# print the JSON string representation of the object
print(AddPartitionSpecUpdate.to_json())

# convert the object into a dict
add_partition_spec_update_dict = add_partition_spec_update_instance.to_dict()
# create an instance of AddPartitionSpecUpdate from a dict
add_partition_spec_update_from_dict = AddPartitionSpecUpdate.from_dict(add_partition_spec_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


