# CreatePrincipalRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal** | [**Principal**](Principal.md) |  | [optional] 
**credential_rotation_required** | **bool** | If true, the initial credentials can only be used to call rotateCredentials | [optional] 

## Example

```python
from polaris.management.models.create_principal_request import CreatePrincipalRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreatePrincipalRequest from a JSON string
create_principal_request_instance = CreatePrincipalRequest.from_json(json)
# print the JSON string representation of the object
print(CreatePrincipalRequest.to_json())

# convert the object into a dict
create_principal_request_dict = create_principal_request_instance.to_dict()
# create an instance of CreatePrincipalRequest from a dict
create_principal_request_from_dict = CreatePrincipalRequest.from_dict(create_principal_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


