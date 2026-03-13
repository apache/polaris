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

# Prerequisites
Ranger authorizer for Polaris requires Apache Ranger version 2.8.0 or later.

# Setup instructions
1. Enable Ranger authorizer by setting the following property in application.properties:
```
polaris.authorization.type=ranger
```

2. Configure Ranger authorizer by setting configurations having name that start with "polaris.authorization.ranger." in application.properties, for example:
```
polaris.authorization.ranger.service-name=dev_polaris
polaris.authorization.ranger."ranger.authz.default.policy.source.impl"=org.apache.ranger.admin.client.RangerAdminRESTClient
polaris.authorization.ranger."ranger.authz.default.policy.rest.url"=http://ranger-admin:6080
polaris.authorization.ranger."ranger.authz.audit.destination.solr"=enabled
polaris.authorization.ranger."ranger.authz.audit.destination.solr.urls"=http://solr-service:8983/solr/ranger_audits

```

3. Run or restart Polaris to see that all accesses are authorized by Ranger policies, with access audit records available in Apache Ranger console.
