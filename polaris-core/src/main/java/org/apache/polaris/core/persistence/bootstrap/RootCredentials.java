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
package org.apache.polaris.core.persistence.bootstrap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableRootCredentials.class)
@JsonDeserialize(as = ImmutableRootCredentials.class)
@tools.jackson.databind.annotation.JsonSerialize(as = ImmutableRootCredentials.class)
@tools.jackson.databind.annotation.JsonDeserialize(as = ImmutableRootCredentials.class)
public interface RootCredentials {

  @Value.Parameter(order = 1)
  @JsonProperty("client-id")
  String clientId();

  @Value.Parameter(order = 2)
  @Value.Redacted
  @JsonProperty("client-secret")
  String clientSecret();

  @Value.Check
  default void check() {
    if (clientId().isEmpty()) {
      throw new IllegalArgumentException("clientId cannot be empty");
    }
    if (clientSecret().isEmpty()) {
      throw new IllegalArgumentException("clientSecret cannot be empty");
    }
  }
}
