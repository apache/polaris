# BaseUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 

## Example

```python
from polaris.catalog.models.base_update import BaseUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of BaseUpdate from a JSON string
base_update_instance = BaseUpdate.from_json(json)
# print the JSON string representation of the object
print(BaseUpdate.to_json())

# convert the object into a dict
base_update_dict = base_update_instance.to_dict()
# create an instance of BaseUpdate from a dict
base_update_from_dict = BaseUpdate.from_dict(base_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


