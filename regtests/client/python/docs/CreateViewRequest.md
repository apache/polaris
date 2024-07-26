# CreateViewRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**location** | **str** |  | [optional] 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**view_version** | [**ViewVersion**](ViewVersion.md) |  | 
**properties** | **Dict[str, str]** |  | 

## Example

```python
from polaris.catalog.models.create_view_request import CreateViewRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateViewRequest from a JSON string
create_view_request_instance = CreateViewRequest.from_json(json)
# print the JSON string representation of the object
print(CreateViewRequest.to_json())

# convert the object into a dict
create_view_request_dict = create_view_request_instance.to_dict()
# create an instance of CreateViewRequest from a dict
create_view_request_from_dict = CreateViewRequest.from_dict(create_view_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


