---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_kms
build:
  list: never
  render: never
---

Server-local KMS credentials configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.kms.aws.access-key` |  | `string` | The AWS access key to use for KMS authentication. If not present, the default credentials  provider chain will be used.  |
| `polaris.kms.aws.secret-key` |  | `string` | The AWS secret key to use for KMS authentication. If not present, the default credentials  provider chain will be used.  |
| `polaris.kms.aws.`_`<kms>`_`.access-key` |  | `string` | The AWS access key to use for KMS authentication when using named KMS configurations.  |
| `polaris.kms.aws.`_`<kms>`_`.secret-key` |  | `string` | The AWS secret key to use for KMS authentication when using named KMS configurations.  |
