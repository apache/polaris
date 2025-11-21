/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Maintenance operations include a bunch of tasks that are regularly executed against a backend
 * database.
 *
 * <p>Types of maintenance operations include:
 *
 * <ul>
 *   <li>Purging unreferenced objects and references within a catalog
 *   <li>Purging whole catalogs that are marked to be purged
 *   <li>Purging whole realms that are marked to be purged
 * </ul>
 *
 * <h2>Discussion
 *
 * <p>Not all databases offer support to perform "prefix key" deletions, which are, for example,
 * necessary to purge a whole realm. Some databases do support "deleting a huge number of rows".
 * Some have another API for prefix-key deletions, for example, Google's BigTable {@code
 * dropRowRange} on the table-admin-client. Relational databases may require different
 * configurations with respect to isolation level to run those maintenance operations in a "better"
 * way. Some databases do not support such "prefix-key deletions" at all, for example, Apache
 * Cassandra or RocksDb or Amazon's DynamoDb.
 *
 * <p>{@link org.apache.polaris.persistence.nosql.api.backend.Backend Backend} implementations
 * therefore expose whether it can leverage "prefix-key deletions" when one or more realms are to be
 * purged. If a {@code Backend} does not support "prefix-key deletions", the whole repository has to
 * be scanned.
 *
 * <h2>Purging unreferenced data
 *
 * <p>The other maintenance operations like purging a catalog or unreferenced objects or references
 * a two-step approach that works even for large multi-tenant setups:
 *
 * <ol>
 *   <li>Memoize the current timestamp, subtract some amount to account for expected wall-clock
 *       drifts.
 *   <li>Identify all objects and references that must be retained, memoize those in a probabilistic
 *       data structure (bloom filter). See below.
 *   <li>Scan the whole database to identify the objects and references that were not identified as
 *       being referenced in the previous step.
 *   <li>Delete the unreferenced objects and references if, and only if, their {@link
 *       org.apache.polaris.persistence.nosql.api.obj.Obj#createdAtMicros() createdAtMicros()}
 *       timestamp is less than the timestamp memoized in the first step.
 * </ol>
 *
 * <h2>Identifying objects and references
 *
 * <p>Implementations of {@link jakarta.enterprise.context.ApplicationScoped @ApplicationScoped}
 * {@link org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier
 * PerRealmRetainedIdentifier} are called to identify the references and objects that have to be
 * retained for a realm.
 *
 * <p>Implementations of {@link jakarta.enterprise.context.ApplicationScoped @ApplicationScoped}
 * {@link org.apache.polaris.persistence.nosql.maintenance.spi.ObjTypeRetainedIdentifier
 * ObjTypeRetainedIdentifier} are called for each identified object of the requested object type.
 *
 * <h2>Realm status
 *
 * <p>The maintenance service implementation will check the current {@linkplain
 * org.apache.polaris.persistence.nosql.realms.api.RealmDefinition#status() status} of the realm to
 * retain and to purge, that the status is valid for being retained (valid: {@linkplain
 * org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus#ACTIVE ACTIVE} and
 * {@linkplain org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus#INACTIVE
 * INACTIVE}) and being purged (valid: {@linkplain
 * org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus#PURGING PURGING}).
 * Realms that have been asked to be purged and for which no data has been encountered will be
 * state-transitioned to {@linkplain
 * org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus#PURGED PURGED}.
 *
 * <h2>System realm {@value org.apache.polaris.persistence.nosql.api.Realms#SYSTEM_REALM_ID}
 *
 * <p>The system realm is maintained like every other realm.
 *
 * <h2>Future export use cases (TBD/TBC)
 *
 * <p>These can be useful in a hosted and multi-tenant SaaS environment, when an export of the data
 * for a particular realm is requested.
 *
 * <ul>
 *   <li>Export live/referenced objects, filtered by realm. A possible implementation would hook
 *       into the implementation of {@link
 *       org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier
 *       PerRealmRetainedIdentifier} via a delegate over {@link
 *       org.apache.polaris.persistence.nosql.api.Persistence Persistence}. The actual approach and
 *       implementation is therefore out of the scope of the maintenance service.
 *   <li>Low-level export, filtered by realm. This one is different from the one above, as it would
 *       export references and all object-<em>parts</em>, in contrast to fully materialized objects.
 *       A possible implementation would hook into the scanning-part of the maintenance service
 *       implementation.
 * </ul>
 */
package org.apache.polaris.persistence.nosql.maintenance.api;
