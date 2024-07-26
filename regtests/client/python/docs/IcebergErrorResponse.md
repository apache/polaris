# IcebergErrorResponse

JSON wrapper for all error responses (non-2xx)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error** | [**ErrorModel**](ErrorModel.md) |  | 

## Example

```python
from polaris.catalog.models.iceberg_error_response import IcebergErrorResponse

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergErrorResponse from a JSON string
iceberg_error_response_instance = IcebergErrorResponse.from_json(json)
# print the JSON string representation of the object
print(IcebergErrorResponse.to_json())

# convert the object into a dict
iceberg_error_response_dict = iceberg_error_response_instance.to_dict()
# create an instance of IcebergErrorResponse from a dict
iceberg_error_response_from_dict = IcebergErrorResponse.from_dict(iceberg_error_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


