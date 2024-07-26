# UpdatePrincipalRequest

Updates to apply to a Principal

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**current_entity_version** | **int** | The version of the object onto which this update is applied; if the object changed, the update will fail and the caller should retry after fetching the latest version. | 
**properties** | **Dict[str, str]** |  | 

## Example

```python
from polaris.management.models.update_principal_request import UpdatePrincipalRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdatePrincipalRequest from a JSON string
update_principal_request_instance = UpdatePrincipalRequest.from_json(json)
# print the JSON string representation of the object
print(UpdatePrincipalRequest.to_json())

# convert the object into a dict
update_principal_request_dict = update_principal_request_instance.to_dict()
# create an instance of UpdatePrincipalRequest from a dict
update_principal_request_from_dict = UpdatePrincipalRequest.from_dict(update_principal_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


