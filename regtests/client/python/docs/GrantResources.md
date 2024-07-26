# GrantResources


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**grants** | [**List[GrantResource]**](GrantResource.md) |  | 

## Example

```python
from polaris.management.models.grant_resources import GrantResources

# TODO update the JSON string below
json = "{}"
# create an instance of GrantResources from a JSON string
grant_resources_instance = GrantResources.from_json(json)
# print the JSON string representation of the object
print(GrantResources.to_json())

# convert the object into a dict
grant_resources_dict = grant_resources_instance.to_dict()
# create an instance of GrantResources from a dict
grant_resources_from_dict = GrantResources.from_dict(grant_resources_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


