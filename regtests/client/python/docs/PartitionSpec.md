# PartitionSpec


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spec_id** | **int** |  | [optional] [readonly] 
**fields** | [**List[PartitionField]**](PartitionField.md) |  | 

## Example

```python
from polaris.catalog.models.partition_spec import PartitionSpec

# TODO update the JSON string below
json = "{}"
# create an instance of PartitionSpec from a JSON string
partition_spec_instance = PartitionSpec.from_json(json)
# print the JSON string representation of the object
print(PartitionSpec.to_json())

# convert the object into a dict
partition_spec_dict = partition_spec_instance.to_dict()
# create an instance of PartitionSpec from a dict
partition_spec_from_dict = PartitionSpec.from_dict(partition_spec_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


