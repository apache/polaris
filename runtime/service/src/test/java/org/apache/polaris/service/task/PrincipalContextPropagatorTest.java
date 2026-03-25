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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
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
  void testCaptureWhenPrincipalResolvableReturnsClonedPrincipal() {
    PolarisPrincipal original = PolarisPrincipal.of("alice", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(true);
    when(principalInstance.get()).thenReturn(original);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    Object state = propagator.capture();
    assertThat(state).isNotNull();
    assertThat(state).isInstanceOf(ImmutablePolarisPrincipal.class);
    assertThat(state).isNotSameAs(original);
    assertThat(((PolarisPrincipal) state).getName()).isEqualTo("alice");
  }

  @Test
  void testCaptureWhenPrincipalNotResolvableReturnsNull() {
    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(false);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    Object state = propagator.capture();
    assertThat(state).isNull();
  }

  @Test
  void testRestoreSetsPrincipalInHolder() throws Exception {
    PolarisPrincipal principal =
        ImmutablePolarisPrincipal.builder()
            .from(PolarisPrincipal.of("carol", Map.of(), Set.of()))
            .build();

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    try (AutoCloseable scope = propagator.restore(principal)) {
      assertThat(scope).isNotNull();
    }

    // Calling restore() a second time on a fresh holder must succeed.
    PolarisPrincipalHolder freshHolder = new PolarisPrincipalHolder();
    PrincipalContextPropagator freshPropagator =
        new PrincipalContextPropagator(freshHolder, principalInstance);
    freshPropagator.restore(principal).close();
  }

  @Test
  void testRestoreNullStateDoesNotThrow() throws Exception {
    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    AutoCloseable scope = propagator.restore(null);
    assertThat(scope).isNotNull();
    scope.close();
  }

  @Test
  void testCaptureWhenScopeNotActiveReturnsNull() {
    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(true);
    when(principalInstance.get()).thenThrow(new ContextNotActiveException());

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    assertThat(propagator.capture()).isNull();
  }

  @Test
  void testRestoreCalledTwiceOnSameHolderThrows() throws Exception {
    PolarisPrincipal principal =
        ImmutablePolarisPrincipal.builder()
            .from(PolarisPrincipal.of("dave", Map.of(), Set.of()))
            .build();

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    propagator.restore(principal).close();
    // Second restore on the same holder must throw because set() uses compareAndSet.
    assertThrows(IllegalStateException.class, () -> propagator.restore(principal));
  }
}
