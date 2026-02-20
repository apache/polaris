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
package org.apache.polaris.core.persistence.resolver;

import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Simple class to represent the name of an entity to resolve */
@PolarisImmutable
public interface ResolverEntityName {

  /** Type of the entity. */
  @Value.Parameter(order = 0)
  PolarisEntityType entityType();

  /** The name of the entity. */
  @Value.Parameter(order = 1)
  String entityName();

  /** True if we should not fail while resolving this entity. Does not count for equals/hashCode. */
  @Value.Auxiliary
  @Value.Parameter(order = 2)
  boolean optional();
}
