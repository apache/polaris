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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Set;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.junit.jupiter.api.Test;

public class BasicResolutionSemanticsTest {
  private static final PolarisAuthorizableOperation OPERATION =
      PolarisAuthorizableOperation.LIST_CATALOGS;
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("test", AttributeMap.EMPTY, Set.of());

  @Test
  void targetlessIntentSelectsTopLevelEntities() {
    assertSelections(
        Set.of(REQUESTED_TOP_LEVEL_ENTITIES), new TargetlessAuthorizationIntent(OPERATION));
  }

  @Test
  void topLevelSecurableSelectsTopLevelEntities() {
    PolarisSecurable principalRole =
        PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "role"));

    assertSelections(
        Set.of(REQUESTED_TOP_LEVEL_ENTITIES),
        new SingleTargetAuthorizationIntent(OPERATION, principalRole));
  }

  @Test
  void catalogScopedSecurableSelectsReferenceCatalogAndRequestedPaths() {
    PolarisSecurable table =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "namespace"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table"));

    assertSelections(
        Set.of(REFERENCE_CATALOG, REQUESTED_PATHS),
        new SingleTargetAuthorizationIntent(OPERATION, table));
  }

  @Test
  void multipleSecurablesAccumulateSelections() {
    PolarisSecurable catalog =
        PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"));
    PolarisSecurable principalRole =
        PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "role"));

    assertSelections(
        Set.of(REFERENCE_CATALOG, REQUESTED_PATHS, REQUESTED_TOP_LEVEL_ENTITIES),
        new PrivilegeGrantAuthorizationIntent(OPERATION, catalog, principalRole));
  }

  private static void assertSelections(Set<Resolvable> expected, AuthorizationIntent... intents) {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    AuthorizationRequest request = new AuthorizationRequest(PRINCIPAL, List.of(intents));

    BasicResolutionSemantics.resolveSelections(authzState, request);
    authzState.resolve();

    verify(manifest).resolveSelections(expected);
  }
}
