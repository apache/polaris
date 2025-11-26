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
 * Distributed cache invalidation for the NoSQL cache.
 *
 * <p>Provides both a receiver and a sender for {@link
 * org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations cache invalidation messages}.
 *
 * <p>The receiver registers a route on the Quarkus management interface. Senders emit asynchronous
 * requests to the cache-invalidation management endpoint.
 *
 * <p>Each Polaris instance submits its ephemeral instance-ID in the invalidation messages to
 * prevent the risk of processing loopback messages as a safety net.
 *
 * <p>All polaris instances share a common token (dynamically generated via helm chart mechanisms)
 * to protect against externally injected, malicious invalidation messages.
 */
package org.apache.polaris.persistence.nosql.quarkus.distcache;
