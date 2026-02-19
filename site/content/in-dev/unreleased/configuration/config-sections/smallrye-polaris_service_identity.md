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
title: smallrye-polaris_service_identity
build:
  list: never
  render: never
---

Configuration interface for managing service identities across multiple realms in Polaris. 

A service identity represents the Polaris service itself when it needs to authenticate to  external systems (e.g., AWS services for SigV4 authentication). Each realm can configure its own  set of service identities for different cloud providers.   

This interface supports multi-tenant deployments where each realm (tenant) can have distinct  service identities, as well as single-tenant deployments with a default configuration shared  across all catalogs.   

Configuration is loaded from `polaris.service-identity.*` properties at startup and  includes credentials that Polaris uses to assume customer-provided roles when accessing federated  catalogs.   

**Example Configuration:** 

```
 # Default service identity (used when no realm-specific configuration exists)
 polaris.service-identity.aws-iam.iam-arn=arn:aws:iam::123456789012:user/polaris-default-user
 # Optional: provide static credentials, or omit to use AWS default credential chain
 polaris.service-identity.aws-iam.access-key-id=<access-key-id>
 polaris.service-identity.aws-iam.secret-access-key=<secret-access-key>
 polaris.service-identity.aws-iam.session-token=<optional-session-token>
 # Realm-specific service identity for multi-tenant deployments
 polaris.service-identity.my-realm.aws-iam.iam-arn=arn:aws:iam::123456789012:user/my-realm-user
 polaris.service-identity.my-realm.aws-iam.access-key-id=<access-key-id>
 polaris.service-identity.my-realm.aws-iam.secret-access-key=<secret-access-key>
 ```

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.service-identity.aws-iam.iam-arn` |  | `string` | The IAM role or user ARN representing the service identity. If not provided, Polaris won't  surface it in the catalog identity.  |
| `polaris.service-identity.aws-iam.access-key-id` |  | `string` | Optional AWS access key ID associated with the IAM identity. If not provided, the AWS default  credential chain will be used.  |
| `polaris.service-identity.aws-iam.secret-access-key` |  | `string` | Optional AWS secret access key associated with the IAM identity. If not provided, the AWS  default credential chain will be used.  |
| `polaris.service-identity.aws-iam.session-token` |  | `string` | Optional AWS session token associated with the IAM identity. If not provided, the AWS default  credential chain will be used.  |
| `polaris.service-identity.`_`<realm>`_`.aws-iam.iam-arn` |  | `string` | The IAM role or user ARN representing the service identity. If not provided, Polaris won't  surface it in the catalog identity.  |
| `polaris.service-identity.`_`<realm>`_`.aws-iam.access-key-id` |  | `string` | Optional AWS access key ID associated with the IAM identity. If not provided, the AWS default  credential chain will be used.  |
| `polaris.service-identity.`_`<realm>`_`.aws-iam.secret-access-key` |  | `string` | Optional AWS secret access key associated with the IAM identity. If not provided, the AWS  default credential chain will be used.  |
| `polaris.service-identity.`_`<realm>`_`.aws-iam.session-token` |  | `string` | Optional AWS session token associated with the IAM identity. If not provided, the AWS default  credential chain will be used.  |
