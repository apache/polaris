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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

class PolarisResolutionManifestTest {

  @Test
  void accessingResultsBeforeResolveAllFailsFast() {
    PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    RealmContext realmContext = () -> "test-realm";
    PolarisPrincipal principal = mock(PolarisPrincipal.class);

    Resolver resolver = mock(Resolver.class);
    ResolverFactory resolverFactory = mock(ResolverFactory.class);
    when(resolverFactory.createResolver(principal, "test-catalog")).thenReturn(resolver);

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            diagnostics, realmContext, resolverFactory, principal, "test-catalog");

    assertThrows(NullPointerException.class, () -> manifest.getResolvedPath("key"));
    assertThrows(NullPointerException.class, () -> manifest.getResolvedReferenceCatalogEntity());
    assertThrows(NullPointerException.class, () -> manifest.getLeafSubType("key"));
    assertThrows(
        NullPointerException.class,
        () -> manifest.getResolvedTopLevelEntity("catalog", PolarisEntityType.CATALOG));
  }
}
