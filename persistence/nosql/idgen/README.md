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

# Unique ID generation framework and monotonic clock

Provides a framework and implementations for unique ID generation, including a monotonically increasing timestamp/clock
source.

Provides a
[Snowflake-IDs](https://medium.com/@jitenderkmr/demystifying-snowflake-ids-a-unique-identifier-in-distributed-computing-72796a827c9d)
implementation.

Consuming production should primarily leverage the `IdGenerator` and `MonotonicClock` interfaces.

## Snowflake ID source

The Snowflake ID source is configurable for each backend instance, but cannot be modified for an existing backend
instance to prevent ID conflicts.

The epoch of these timestamps is 2025-03-01-00:00:00.0 GMT. Timestamps occupy 41 bits at
millisecond precision, which lasts for about 69 years. Node-IDs are 10 bits, which allows 1024 concurrently active
"JVMs running Polaris". 12 bits are used by the sequence number, which then allows each node to generate 4096 IDs per
millisecond. One bit is reserved for future use.

Node IDs are leased by every "JVM running Polaris" for a period of time. The ID generator implementation guarantees
that no IDs will be generated for a timestamp that exceeds the "lease time". Leases can be extended. The implementation
leverages atomic database operations (CAS) for the lease implementation.

ID generators must not use timestamps before or after the lease period nor must they re-use an older timestamp. This
requirement is satisfied using a monotonic clock implementation.

## Code structure

The code is structured into multiple modules. Consuming code should almost always pull in only the API module.

* `polaris-idgen-api` provides the necessary Java interfaces and immutable types.
* `polaris-idgen-impl` provides the storage agnostic implementation.
* `polaris-idgen-mocks` provides mocks for testing.
* `polaris-idgen-spi` provides the necessary interfaces to construct ID generators.
