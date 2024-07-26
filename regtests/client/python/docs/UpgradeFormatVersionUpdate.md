# UpgradeFormatVersionUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**format_version** | **int** |  | 

## Example

```python
from polaris.catalog.models.upgrade_format_version_update import UpgradeFormatVersionUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of UpgradeFormatVersionUpdate from a JSON string
upgrade_format_version_update_instance = UpgradeFormatVersionUpdate.from_json(json)
# print the JSON string representation of the object
print(UpgradeFormatVersionUpdate.to_json())

# convert the object into a dict
upgrade_format_version_update_dict = upgrade_format_version_update_instance.to_dict()
# create an instance of UpgradeFormatVersionUpdate from a dict
upgrade_format_version_update_from_dict = UpgradeFormatVersionUpdate.from_dict(upgrade_format_version_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


