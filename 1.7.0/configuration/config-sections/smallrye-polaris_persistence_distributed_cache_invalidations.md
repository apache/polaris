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
title: smallrye-polaris_persistence_distributed_cache_invalidations
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.distributed-cache-invalidations.service-names` |  | `list of string` | Host names or IP addresses or kubernetes headless-service name of all Polaris server instances  accessing the same repository.  <br><br>This value is automatically configured via the Polaris Helm chart, additional configuration  is not required.   <br><br>If you have your own Helm chart or custom deployment, make sure to configure the IPs of all  Polaris instances here.   <br><br>Names that start with an equal sign are not resolved but used "as is". |
| `polaris.persistence.distributed-cache-invalidations.valid-tokens` |  | `list of string` | List of cache-invalidation tokens to authenticate incoming cache-invalidation messages. <br><br>The first token is used in outgoing cache-invalidation messages. |
| `polaris.persistence.distributed-cache-invalidations.uri` | `/polaris-management/cache-coherency` | `string` | URI of the cache-invalidation endpoint, only available on the Quarkus management port, defaults  to 9000.   |
| `polaris.persistence.distributed-cache-invalidations.service-name-lookup-interval` | `PT10S` | `duration` | Interval of service-name lookups to resolve the service names (#cacheInvalidationServiceNames()) into IP addresses.   |
| `polaris.persistence.distributed-cache-invalidations.batch-size` | `20` | `int` | Maximum number of cache-invalidation messages to send in a single request to peer nodes.  |
| `polaris.persistence.distributed-cache-invalidations.request-timeout` |  | `duration` | Request timeout for sent cache-invalidation messages. Timeouts trigger a warning or error  message.  |
| `polaris.persistence.distributed-cache-invalidations.dns.query-timeout` | `PT5S` | `duration` | Timeout for DNS queries to resolve peer nodes.  |
