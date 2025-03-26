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

# AuthZ framework with pluggable privileges

Provides a framework and implementations pluggable privileges and privilege checks.

## Privileges

A privilege is globally identified by its name. Privileges can be inheritable (from its parents) or not. Multiple
privileges can be grouped together to a _composite_ privilege (think: `ALL_DML` having `SELECT`, `INSERT`, `UPDATE` and
`DELETE`) - a composite privilege matches, if all its individual privileges match. Multiple privileges can also be
grouped to an _alternative_ privilege, which matches if any of its individual privileges matches.

Available privileges are provided by one or more `PrivilegeProvider`s, which are discovered at runtime.
Note: currently there is only one `ProvilegeProvider` that plugs in the Polaris privileges.

## ACLs, ACL entries and ACL chains

Each securable object can have its own ACL. ACLs consist of ACL entries, which define the _granted_ and _restricted_
privileges by role name. The the number of roles is technically unbounded and the number of ACL entries can become
quite large.

This framework implements [separation of duties](https://en.wikipedia.org/wiki/Separation_of_duties) ("SoD"), which is a
quite demanded functionality not just by large(r) user organizations. TL;DR _SoD_ allows "security administrators" to
grant and revoke privileges to other users, but not leverage those privileges themselves.

The _effective_ set of privileges for a specific operation performed by a specific caller needs to be computed against
the target objects and their parents. _ACL chains_ are the vehicle to model this hierarchy and let the implementation
compute the set of _effective_ privileges based on the individual ACLs and roles.

Note: Privilege checks and _SoD_ are currently not performed via this framework.

## Jackson support & Storage friendly representation

The persistable types `Acl`, `AclEntry`, and `PrivilegeSet` can all be serialized using Jackson.

As the number of ACL entries can become quite large, space efficient serialization is quite important. The
implementation uses bit-set encoding when serializing `PrivilegeSet`s for persistence.

## Code structure

The code is structured into multiple modules. Consuming code should almost always pull in only the API module.

* `polaris-authz-api` provides the necessary Java interfaces and immutable types.
* `polaris-authz-impl` provides the storage agnostic implementation.
* `polaris-authz-spi` provides the necessary interfaces to provide custom privileges and storage implementation.
* `polaris-authz-store-nosql` provides the storage implementation based on `polaris-persistence-nosql-api`.
