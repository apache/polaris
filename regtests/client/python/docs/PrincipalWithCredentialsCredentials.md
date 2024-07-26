# PrincipalWithCredentialsCredentials


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**client_id** | **str** |  | [optional] 
**client_secret** | **str** |  | [optional] 

## Example

```python
from polaris.management.models.principal_with_credentials_credentials import PrincipalWithCredentialsCredentials

# TODO update the JSON string below
json = "{}"
# create an instance of PrincipalWithCredentialsCredentials from a JSON string
principal_with_credentials_credentials_instance = PrincipalWithCredentialsCredentials.from_json(json)
# print the JSON string representation of the object
print(PrincipalWithCredentialsCredentials.to_json())

# convert the object into a dict
principal_with_credentials_credentials_dict = principal_with_credentials_credentials_instance.to_dict()
# create an instance of PrincipalWithCredentialsCredentials from a dict
principal_with_credentials_credentials_from_dict = PrincipalWithCredentialsCredentials.from_dict(principal_with_credentials_credentials_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


