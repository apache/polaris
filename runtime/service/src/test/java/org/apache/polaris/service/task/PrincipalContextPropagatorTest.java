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
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PrincipalContextPropagator}. */
class PrincipalContextPropagatorTest {

  private PolarisPrincipalHolder holder;

  @BeforeEach
  void setUp() {
    holder = new PolarisPrincipalHolder();
  }

  @Test
  void testCaptureWhenPrincipalResolvableReturnsAction() {
    PolarisPrincipal original = PolarisPrincipal.of("alice", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.get()).thenReturn(original);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();
  }

  @Test
  void testRestoreSetsPrincipalInHolder() throws Exception {
    PolarisPrincipal original = PolarisPrincipal.of("carol", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.get()).thenReturn(original);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();

    action.restore();
    action.close();

    // Calling capture + restore on a fresh holder must succeed.
    PolarisPrincipalHolder freshHolder = new PolarisPrincipalHolder();
    PrincipalContextPropagator freshPropagator =
        new PrincipalContextPropagator(freshHolder, principalInstance);
    AsyncContextPropagator.RestoreAction freshAction = freshPropagator.capture();
    assertThat(freshAction).isNotNull();
    freshAction.restore();
    freshAction.close();
  }

  @Test
  void testCaptureWhenScopeNotActiveReturnsNoop() {
    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.get()).thenThrow(new ContextNotActiveException());

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    assertThat(propagator.capture()).isSameAs(AsyncContextPropagator.RestoreAction.NOOP);
  }

  @Test
  void testCloseIsNoOp() throws Exception {
    PolarisPrincipal original = PolarisPrincipal.of("dave", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.get()).thenReturn(original);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();
    action.restore();
    action.close(); // no-op; should not throw
  }
}
