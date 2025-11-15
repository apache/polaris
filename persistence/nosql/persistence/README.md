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

# Database agnostic persistence framework

The NoSQL persistence API and functional implementations are based on the assumption that all databases targeted as
backing stores for Polaris support "compare and swap" operations on a single row.
These CAS operations are the only requirement.

Since some databases do enforce hard size limits, for example, DynamoDB has a hard 400kB row size limit.
MariaDB/MySQL has a default 512kB packet size limit.
Other databases have row-size recommendations around similar sizes.
Polaris persistence respects those limits and recommendations using a common hard limit of 350kB.

Objects exposed via the `Persistence` interface are typed Java objects that must be immutable and serializable using
Jackson.
Each type is described via an implementation of the `ObjType` interface using a name, which must be unique
in Polaris, and a target Java type: the Jackson serializable Java type.
Object types are registered using the Java service API using `ObjType`.
The actual java target types must extend the `Obj` interface.
The (logical) key for each `Obj` is a composite of the `ObjType.id()` and a `long` ID (64-bit signed int),
combined using the `ObjId` composite type.

The "primary key" of each object in a database is always _realmId + object-ID_, where realm-ID is a string and
object-ID is a 64-bit integer.
This allows, but does not enforce, storing multiple realms in one backend database.

Data in/for/of each Polaris realm (think: _tenant_) is isolated using the realm's ID (string).
The base `Persistence` API interface is always scoped to exactly one realm ID.

## Supporting more databases

The code to support a particular database is isolated in a project, for example `polaris-persistence-nosql-inmemory` and
`polaris-persistence-nosql-mongodb`.

When adding another database, it must also be wired up to Quarkus in `polaris-persistence-nosql-cdi-quarkus` preferably
using Quarkus extensions, added to the `polaris-persitence-corectness` tests and available in
`polaris-persistence-nosql-benchmark` for low-level benchmarks.

## Named pointers

Polaris represents a catalog for data lakehouses, which means that the information of and for catalog entities like
Iceberg tables, views, and namespaces must be consistent, even if multiple catalog entities are changes in a single
atomic operation.

Polaris leverages a concept called "Named pointers."
The state of the whole catalog is referenced via the so-called HEAD (think: Git HEAD),
which _points to_ all catalog entities.
This state is persisted as an `Obj` with an index of the catalog entities,
the ID of that "current catalog state `Obj`" is maintained in one named pointer.

Named pointers are also used for other purposes than catalog entities, for example, to maintain realms or
configurations.

## Committing changes

Changes are persisted using a commit mechanism, providing atomic changes across multiple entities against one named
pointer.
The logic implementation ensures that even high-frequency concurrent changes do neither let clients fail
nor cause timeouts.
The behavior and achievable throughput depend on the database being used; some databases perform
_much_ better than others.

A use-case agnostic "committer" abstraction exists to ease implementing committing operations.
For catalog operations, there is a more specialized abstraction.

## `long` IDs

Polaris NoSQL persistence uses so-called Snowflake IDs, which are 64-bit integers that represent a timestamp, a
node-ID, and a sequence number.
The epoch of these timestamps is 2025-03-01-00:00:00.0 GMT.
Timestamps occupy 41 bits at millisecond precision, which lasts for about 69 years.
Node-IDs are 10 bits, which allows 1024 concurrently active "JVMs running Polaris."
Twelve (12) bits are used by the sequence number, which then allows each node to generate 4096 IDs per
millisecond.
One bit is reserved for future use.

Node IDs are leased by every "JVM running Polaris" for a period of time.
The ID generator implementation guarantees that no IDs will be generated for a timestamp that exceeds the "lease time."
Leases can be extended.
The implementation leverages atomic database operations (CAS) for the lease implementation.

ID generators must not use timestamps before or after the lease period, nor must they re-use an older timestamp.
This requirement is satisfied using a monotonic clock implementation.

## Caching

Since most `Obj`s are by default assumed to be immutable, caching is very straight forward and does not require any
coordination, which simplifies the design and implementation quite a bit.

## Strong vs. eventual consistency

Polaris NoSQL persistence offers two ways to persist `Obj`s: strongly consistent and eventually consistent.
The former is slower than the latter.

Since Polaris NoSQL persistence respects the hard size limitations mentioned above, it cannot persist the serialized
representation of objects that exceed those limits in a single database row.
However, some objects legibly exceed those limits.
Polaris NoSQL persistence allows such "big object serializations" and writes those into multiple database rows,
with the restriction that this is only supported for eventually consistent write operations.
The serialized representation for strong consistency writes must always be within the hard limit.

## Indexes

The state of a data-lakehouse catalog can contain many thousand, potentially a few 100,000, tables/views/namespaces.
Even space-efficient serialization of an index for that many entries can exceed the "common hard 350kB limit."
New changes end in the index, which is "embedded" in the "current catalog state `Obj`".
If the respective index size limit of this "embedded" index is being approached,
the index is spilled out to separate rows in the database.
The implementation is built to split and combine when needed.

## Change log / events / notifications

The commit mechanism described above builds a commit log.
All changes can be inspected via that log in exactly the order in which those happened (think: `git log`).
Since the log of changes is already present, it is possible to retrieve the changes from some point in time or
commit log ID.
This allows clients to receive all changes that have happened since the last known commit ID,
offering a mechanism to poll for changes.
Since the necessary `Obj`s are immutable,
such change-log-requests likely hit already cached data and rather not the database.

## Clean up old commits / unused data

Despite the beauty of having a "commit log" and all metadata representation in the backing database,
the size of that database would always grow.

Purging unused table/view metadata memoized in the database is one piece.
Purging old commit log entries is the second part.
Purging (then) unreferenced `Obj`s the third part.

See [maintenance service](#maintenance-service) below.

## Realms (aka tenants)

Bootstrapping but more importantly, deleting/purging a realm is a non-trivial operation, which requires its own
lifecycle.
Bootstrapping is a straight forward operation as the necessary information can be validated and enhanced if necessary.

Both the logical but also the physical process of realm deletion are more complex.
From a logical point of view,
users want to disable the realm for a while before they eventually are okay with deleting the information.

The process to delete a realm's data from the database can be quite time-consuming, and how that happens is
database-specific.
While some databases can do bulk-deletions, which "just" take some time (RDBMS, BigTable), other databases
require that the process of deleting a realm must happen during a full scan of the database (for example, RocksDB
and Apache Cassandra).
Since scanning the whole database itself can take quite long, and no more than one instance should scan the database
at any time.

The realm has a status to reflect its lifecycle.
The initial status of a realm is `CREATED`, which effectively only means that the realm-ID has been reserved and that
the necessary data needs to be populated (bootstrap).
Once a realm has been fully bootstrapped, its status is changed to `ACTIVE`.
Only `ACTIVE` realms can be used for user requests.

Between `CREATED` and `ACTIVE`/`INACTIVE` there are two states that are mutually exclusive.
The state `INITIALIZING` means that Polaris will initialize the realm as a fresh, new realm.
The state `LOADING` means that realm data, which has been exported from another Polaris instance, is to be imported.

Realm deletion is a multistep approach as well: Realms are first put into `INACTIVE` state, which can be reverted
to `ACTIVE` state or into `PURGING` state.
The state `PURGING` means that the realm's data is being deleted from the database,
once purging has been started, the realm's information in the database is inconsistent and cannot be restored.
Once the realm's data has been purged, the realm is put into `PURGED` state. Only realms that are in state `PURGED`
can be deleted.

The multi-state approach also prevents that a realm can only be used when the system knows that all necessary
information is present.

**Note**: the realm state machine is not fully implemented yet.

## `::system::` realm

Polaris NoSQL persistence uses a system realm which is used for node ID leases and realm management.
The realm-IDs starting with two colons (`::`) are reserved for system use.

### Named pointers in the `::system::` realm

| Named pointer | Meaning         |
|---------------|-----------------|
| `realms`      | Realms, by name |

## "User" realms

### Named pointers in the user realms

| Named pointer     | Meaning                      |
|-------------------|------------------------------|
| `root`            | Pointer to the "root" entity |
| `catalogs`        | Catalogs                     |
| `principals`      | Principals                   |
| `principal-roles` | Principal roles              |
| `grants`          | All grants                   |
| `immediate-tasks` | Immediately scheduled tasks  |
| `policy-mappings` | Policy mappings              |

Per catalog named pointers, where `%d` refers to the catalog's integer ID:

| Named pointer       | Meaning                                          |
|---------------------|--------------------------------------------------|
| `cat/%d/roles`      | Catalog roles                                    |
| `cat/%d/heads/main` | Catalog content (namespaces, tables, views, etc) |
| `cat/%d/grants`     | Catalog related grants (*)                       |

(*) = currently not used, stored in the realm grants.

## Maintenance Service

**Note**: maintenance service not yet in the code base.

The maintenance service is a mechanism to scan the backend database and perform necessary maintenance operations
as a background service.

The most important maintenance operation is to purge unreferenced objects from the database.
Pluggable "identifiers" are used to "mark" objects to retain.

The implementation calls all per-realm "identifiers," which then "mark" the named pointers and objects that have to be
retained.
Plugins and/or extensions can provide per-object-type "identifiers," which get called for "identified" objects.
The second phase of the maintenance service scans the whole backend database and purges those objects,
which have not been "marked" to be retained.

Maintenance service invocations require two sets of realm-ids: the set of realms to retain and the set of realms
to purge.
These sets can be derived using `RealmManagement.list()` and grouping realms by their status.

### Purging realms

Eventually, purging realm from the backend database can happen in two different ways, depending on the database.
Some databases support deleting one or more realms using bulk deletions.
Other databases do not support this kind of bulk deletion.
Both ways are supported by the maintenance service.

Eventually, purging realms is a responsibility of the [maintenance service](#maintenance-service).
