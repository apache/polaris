# PolarisCatalog

The base catalog type - this contains all the fields necessary to construct an INTERNAL catalog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------

## Example

```python
from polaris.management.models.polaris_catalog import PolarisCatalog

# TODO update the JSON string below
json = "{}"
# create an instance of PolarisCatalog from a JSON string
polaris_catalog_instance = PolarisCatalog.from_json(json)
# print the JSON string representation of the object
print(PolarisCatalog.to_json())

# convert the object into a dict
polaris_catalog_dict = polaris_catalog_instance.to_dict()
# create an instance of PolarisCatalog from a dict
polaris_catalog_from_dict = PolarisCatalog.from_dict(polaris_catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


