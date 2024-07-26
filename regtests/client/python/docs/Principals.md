# Principals

A list of Principals

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principals** | [**List[Principal]**](Principal.md) |  | 

## Example

```python
from polaris.management.models.principals import Principals

# TODO update the JSON string below
json = "{}"
# create an instance of Principals from a JSON string
principals_instance = Principals.from_json(json)
# print the JSON string representation of the object
print(Principals.to_json())

# convert the object into a dict
principals_dict = principals_instance.to_dict()
# create an instance of Principals from a dict
principals_from_dict = Principals.from_dict(principals_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


