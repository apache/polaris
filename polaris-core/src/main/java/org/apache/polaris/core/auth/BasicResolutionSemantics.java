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

package org.apache.polaris.core.auth;

import static org.apache.polaris.core.persistence.resolver.Resolvable.REFERENCE_CATALOG;
import static org.apache.polaris.core.persistence.resolver.Resolvable.REQUESTED_PATHS;
import static org.apache.polaris.core.persistence.resolver.Resolvable.REQUESTED_TOP_LEVEL_ENTITIES;

import org.apache.polaris.core.entity.PolarisEntityType;
import org.jspecify.annotations.NonNull;

public class BasicResolutionSemantics {
  public static void resolveSelections(
      AuthorizationState authzState, AuthorizationRequest request) {
    mergeSelections(authzState, request);
  }

  private static void mergeSelections(
      AuthorizationState authzState, @NonNull PolarisSecurable securable) {
    boolean top = true;
    for (PathSegment path : securable.getPathSegments()) {
      if (path.entityType() == PolarisEntityType.CATALOG) {
        authzState.select(REFERENCE_CATALOG);
        authzState.select(REQUESTED_PATHS);
        top = false;
        break;
      }
    }

    if (top) {
      authzState.select(REQUESTED_TOP_LEVEL_ENTITIES);
    }
  }

  private static void mergeSelections(
      AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    for (AuthorizationIntent intent : request.intents()) {
      if (intent instanceof TargetlessAuthorizationIntent) {
        authzState.select(REQUESTED_TOP_LEVEL_ENTITIES);
      } else {
        intent.visitSecurables((securable) -> mergeSelections(authzState, securable));
      }
    }
  }
}
