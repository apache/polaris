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

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * OPA batch authorization input structure.
 *
 * <p>Carries multiple authorization questions for one actor in a single call. The batch rule in the
 * policy answers each question with the same logic as the single {@code allow} rule and returns a
 * map from question ID to a boolean decision.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableOpaBatchAuthorizationInput.class)
@JsonDeserialize(as = ImmutableOpaBatchAuthorizationInput.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public interface OpaBatchAuthorizationInput {
  /** The actor making all of the authorization requests in this batch. */
  Actor actor();

  /** Additional context about the request, shared by every question in the batch. */
  Context context();

  /** The questions to answer, each identified by a unique {@link OpaBatchQuery#id() id}. */
  List<OpaBatchQuery> requests();
}
