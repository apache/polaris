# ExternalCatalog

An externally managed catalog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**remote_url** | **str** | URL to the remote catalog API | [optional] 

## Example

```python
from polaris.management.models.external_catalog import ExternalCatalog

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalCatalog from a JSON string
external_catalog_instance = ExternalCatalog.from_json(json)
# print the JSON string representation of the object
print(ExternalCatalog.to_json())

# convert the object into a dict
external_catalog_dict = external_catalog_instance.to_dict()
# create an instance of ExternalCatalog from a dict
external_catalog_from_dict = ExternalCatalog.from_dict(external_catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


