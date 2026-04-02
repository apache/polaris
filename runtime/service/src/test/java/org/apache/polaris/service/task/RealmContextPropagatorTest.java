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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ContextNotActiveException;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RealmContextPropagator}. */
class RealmContextPropagatorTest {

  @Test
  void testCaptureWithRealmInHolderReturnsAction() {
    RealmContextHolder holder = new RealmContextHolder();
    RealmContext realmContext = () -> "test-realm";
    holder.set(realmContext);

    RealmContextPropagator propagator = new RealmContextPropagator(holder);
    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();
  }

  @Test
  void testCaptureWithNullRealmInHolderReturnsNoop() {
    RealmContextHolder holder = new RealmContextHolder();
    RealmContextPropagator propagator = new RealmContextPropagator(holder);
    assertThat(propagator.capture()).isSameAs(AsyncContextPropagator.RestoreAction.NOOP);
  }

  @Test
  void testCaptureWhenScopeNotActiveReturnsNoop() {
    RealmContextHolder mockHolder = mock(RealmContextHolder.class);
    when(mockHolder.get()).thenThrow(new ContextNotActiveException());

    RealmContextPropagator propagator = new RealmContextPropagator(mockHolder);
    assertThat(propagator.capture()).isSameAs(AsyncContextPropagator.RestoreAction.NOOP);
  }

  @Test
  void testRestoreSetsRealmInHolder() throws Exception {
    // Use a mock to simulate CDI proxy behavior: get() returns the realm on capture,
    // set() succeeds on restore (as it would on a fresh request-scope holder).
    RealmContext realmContext = () -> "restored-realm";
    RealmContextHolder mockHolder = mock(RealmContextHolder.class);
    when(mockHolder.get()).thenReturn(realmContext);

    RealmContextPropagator propagator = new RealmContextPropagator(mockHolder);
    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();

    action.restore();
    verify(mockHolder).set(realmContext);
  }

  @Test
  void testCloseIsNoOp() throws Exception {
    RealmContext realmContext = () -> "realm";
    RealmContextHolder mockHolder = mock(RealmContextHolder.class);
    when(mockHolder.get()).thenReturn(realmContext);

    RealmContextPropagator propagator = new RealmContextPropagator(mockHolder);
    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();
    action.restore();
    action.close(); // no-op; should not throw
  }
}
