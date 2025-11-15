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

# Dynamic realm management

Framework to manage realms.

## Code structure

The code is structured into multiple modules. Consuming code should almost always pull in only the API module.

* `polaris-persistence-nosql-realms-api` provides the necessary Java interfaces and immutable types.
* `polaris-persistence-nosql-realms-id` provides a type-safe holder for a realm ID.
* `polaris-persistence-nosql-realms-impl` provides the storage agnostic implementation.
* `polaris-persistence-nosql-realms-spi` provides the necessary interfaces to provide a storage specific implementation.
* `polaris-persistence-nosql-realms-store-nosql` provides the storage implementation based on `polaris-persistence-nosql-api`.
