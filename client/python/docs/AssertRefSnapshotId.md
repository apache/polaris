<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->
# AssertRefSnapshotId

The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**ref** | **str** |  | 
**snapshot_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_ref_snapshot_id import AssertRefSnapshotId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertRefSnapshotId from a JSON string
assert_ref_snapshot_id_instance = AssertRefSnapshotId.from_json(json)
# print the JSON string representation of the object
print(AssertRefSnapshotId.to_json())

# convert the object into a dict
assert_ref_snapshot_id_dict = assert_ref_snapshot_id_instance.to_dict()
# create an instance of AssertRefSnapshotId from a dict
assert_ref_snapshot_id_from_dict = AssertRefSnapshotId.from_dict(assert_ref_snapshot_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


