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
package org.apache.polaris.persistence.nosql.coretypes.realm;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Polaris policy external mappings and parameters.
 *
 * <p>{@code null} values are deserialized as an "empty" policy mapping.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutablePolicyMapping.class)
@JsonDeserialize(as = ImmutablePolicyMapping.class)
public interface PolicyMapping {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> parameters();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<ObjRef> externalMapping();

  static ImmutablePolicyMapping.Builder builder() {
    return ImmutablePolicyMapping.builder();
  }

  PolicyMapping EMPTY = builder().parameters(Map.of()).build();

  IndexValueSerializer<PolicyMapping> POLICY_MAPPING_SERIALIZER = new PolicyMappingSerializer();
}
