# RegisterTableRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**metadata_location** | **str** |  | 

## Example

```python
from polaris.catalog.models.register_table_request import RegisterTableRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RegisterTableRequest from a JSON string
register_table_request_instance = RegisterTableRequest.from_json(json)
# print the JSON string representation of the object
print(RegisterTableRequest.to_json())

# convert the object into a dict
register_table_request_dict = register_table_request_instance.to_dict()
# create an instance of RegisterTableRequest from a dict
register_table_request_from_dict = RegisterTableRequest.from_dict(register_table_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


