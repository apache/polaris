# Catalogs

A list of Catalog objects

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalogs** | [**List[Catalog]**](Catalog.md) |  | 

## Example

```python
from polaris.management.models.catalogs import Catalogs

# TODO update the JSON string below
json = "{}"
# create an instance of Catalogs from a JSON string
catalogs_instance = Catalogs.from_json(json)
# print the JSON string representation of the object
print(Catalogs.to_json())

# convert the object into a dict
catalogs_dict = catalogs_instance.to_dict()
# create an instance of Catalogs from a dict
catalogs_from_dict = Catalogs.from_dict(catalogs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


