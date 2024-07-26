# Principal

A Polaris principal.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**client_id** | **str** | The output-only OAuth clientId associated with this principal if applicable | [optional] 
**properties** | **Dict[str, str]** |  | [optional] 
**create_timestamp** | **int** |  | [optional] 
**last_update_timestamp** | **int** |  | [optional] 
**entity_version** | **int** | The version of the principal object used to determine if the principal metadata has changed | [optional] 

## Example

```python
from polaris.management.models.principal import Principal

# TODO update the JSON string below
json = "{}"
# create an instance of Principal from a JSON string
principal_instance = Principal.from_json(json)
# print the JSON string representation of the object
print(Principal.to_json())

# convert the object into a dict
principal_dict = principal_instance.to_dict()
# create an instance of Principal from a dict
principal_from_dict = Principal.from_dict(principal_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


