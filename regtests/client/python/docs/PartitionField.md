# PartitionField


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**field_id** | **int** |  | [optional] 
**source_id** | **int** |  | 
**name** | **str** |  | 
**transform** | **str** |  | 

## Example

```python
from polaris.catalog.models.partition_field import PartitionField

# TODO update the JSON string below
json = "{}"
# create an instance of PartitionField from a JSON string
partition_field_instance = PartitionField.from_json(json)
# print the JSON string representation of the object
print(PartitionField.to_json())

# convert the object into a dict
partition_field_dict = partition_field_instance.to_dict()
# create an instance of PartitionField from a dict
partition_field_from_dict = PartitionField.from_dict(partition_field_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


