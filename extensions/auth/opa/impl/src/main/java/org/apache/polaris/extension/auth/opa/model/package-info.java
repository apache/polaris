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
 * OPA authorization input model classes.
 *
 * <p>This package contains immutable model classes that define the structure of authorization
 * requests sent to Open Policy Agent (OPA). These classes serve as the single source of truth for
 * the OPA input schema.
 *
 * <h2>Schema Generation</h2>
 *
 * <p>The JSON Schema for these models can be generated using the {@link
 * org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator} utility or by running the Gradle
 * task:
 *
 * <pre>{@code
 * ./gradlew :polaris-extensions-auth-opa:generateOpaSchema
 * }</pre>
 *
 * <p>This generates {@code opa-input-schema.json} which can be referenced in documentation and used
 * by OPA policy developers.
 *
 * <h2>Model Structure</h2>
 *
 * <ul>
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.OpaRequest} - Top-level request wrapper
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.OpaAuthorizationInput} - Authorization
 *       context containing actor, action, resource, and context
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.Actor} - Principal and roles
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.Resource} - Target and secondary
 *       resources
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.ResourceEntity} - Individual resource
 *       with type, name, and parents
 *   <li>{@link org.apache.polaris.extension.auth.opa.model.Context} - Request metadata
 * </ul>
 */
package org.apache.polaris.extension.auth.opa.model;
