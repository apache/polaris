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
 * Optional Amazon DynamoDB persistence backend for Polaris.
 *
 * <p>This module provides a {@code BasePersistence} adapter backed by DynamoDB, selected at startup
 * via {@code polaris.persistence.type=dynamodb}. It reuses the existing {@code
 * AtomicOperationMetaStoreManager} (per-entity compare-and-set), mirroring the layering of the
 * {@code relational-jdbc} backend, so the metastore manager, catalog API, and entity model are
 * unchanged.
 *
 * <p>The backend is fully optional and opt-in: deployments that do not select it are unaffected,
 * and no new dependency is imposed on users or downstream projects that do not enable it.
 *
 * <p>The implementation lands incrementally across a series of pull requests; this initial module
 * provides the skeleton and build wiring only.
 */
package org.apache.polaris.persistence.atomic.dynamodb;
