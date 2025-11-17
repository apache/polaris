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
package org.apache.polaris.extension.auth.opa.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Represents a single resource entity in the authorization context.
 *
 * <p>Contains the entity type, name, and hierarchical parent path.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableResourceEntity.class)
@JsonDeserialize(as = ImmutableResourceEntity.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public interface ResourceEntity {
  /** The type of the resource (e.g., "CATALOG", "NAMESPACE", "TABLE"). */
  String type();

  /** The name of the resource. */
  String name();

  /**
   * The hierarchical path of parent entities.
   *
   * <p>For example, a table might have parents: [catalog, namespace].
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  List<ResourceEntity> parents();
}
