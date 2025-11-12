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
package org.apache.polaris.persistence.nosql.authz.spi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;

/**
 * Value type holding the Polaris system-wide mapping of {@linkplain Privilege privilege}
 * {@linkplain Privilege#name() names} to and from integer IDs.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutablePrivilegesMapping.class)
@JsonDeserialize(as = ImmutablePrivilegesMapping.class)
public interface PrivilegesMapping {
  Map<String, Integer> nameToId();

  static ImmutablePrivilegesMapping.Builder builder() {
    return ImmutablePrivilegesMapping.builder();
  }

  PrivilegesMapping EMPTY = PrivilegesMapping.builder().build();
}
