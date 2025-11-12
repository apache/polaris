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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
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
public interface ResourceEntity {
  /** The type of the resource (e.g., "CATALOG", "NAMESPACE", "TABLE"). */
  @JsonProperty("type")
  String type();

  /** The name of the resource. */
  @JsonProperty("name")
  String name();

  /**
   * The hierarchical path of parent entities.
   *
   * <p>For example, a table might have parents: [catalog, namespace].
   */
  @Nullable
  @JsonProperty("parents")
  List<ResourceEntity> parents();
}
