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

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TaskContextPropagator}. */
class TaskContextPropagatorTest {

  @Test
  void testCaptureRealmContext() {
    RealmContext realmContext = () -> "test-realm";
    RealmContextHolder realmHolder = mock(RealmContextHolder.class);
    when(realmHolder.get()).thenReturn(realmContext);

    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of());
    RequestIdHolder requestIdHolder = mock(RequestIdHolder.class);
    when(requestIdHolder.get()).thenReturn("req-1");

    TaskContextPropagator propagator =
        new TaskContextPropagator(
            realmHolder, mock(PolarisPrincipalHolder.class), requestIdHolder, principal);

    TaskContextPropagator.CapturedTaskContext captured = propagator.capture();
    assertThat(captured.realmContext()).isSameAs(realmContext);
    assertThat(captured.realmContext().getRealmIdentifier()).isEqualTo("test-realm");
  }

  @Test
  void testCapturePrincipal() {
    PolarisPrincipal principal = PolarisPrincipal.of("bob", Map.of(), Set.of());
    RealmContextHolder realmHolder = mock(RealmContextHolder.class);
    when(realmHolder.get()).thenReturn(() -> "realm");
    RequestIdHolder requestIdHolder = mock(RequestIdHolder.class);

    TaskContextPropagator propagator =
        new TaskContextPropagator(
            realmHolder, mock(PolarisPrincipalHolder.class), requestIdHolder, principal);

    TaskContextPropagator.CapturedTaskContext captured = propagator.capture();
    assertThat(captured.principal()).isNotSameAs(principal);
    assertThat(captured.principal().getName()).isEqualTo("bob");
  }

  @Test
  void testCaptureRequestId() {
    RealmContextHolder realmHolder = mock(RealmContextHolder.class);
    when(realmHolder.get()).thenReturn(() -> "realm");
    PolarisPrincipal principal = PolarisPrincipal.of("carol", Map.of(), Set.of());
    RequestIdHolder requestIdHolder = mock(RequestIdHolder.class);
    when(requestIdHolder.get()).thenReturn("req-42");

    TaskContextPropagator propagator =
        new TaskContextPropagator(
            realmHolder, mock(PolarisPrincipalHolder.class), requestIdHolder, principal);

    TaskContextPropagator.CapturedTaskContext captured = propagator.capture();
    assertThat(captured.requestId()).isEqualTo("req-42");
  }

  @Test
  void testRestoreSetsAllHolders() {
    RealmContext realmContext = () -> "restored-realm";
    PolarisPrincipal principal = PolarisPrincipal.of("dave", Map.of(), Set.of());

    RealmContextHolder realmHolder = mock(RealmContextHolder.class);
    PolarisPrincipalHolder principalHolder = mock(PolarisPrincipalHolder.class);
    RequestIdHolder requestIdHolder = mock(RequestIdHolder.class);

    TaskContextPropagator propagator =
        new TaskContextPropagator(realmHolder, principalHolder, requestIdHolder, principal);

    TaskContextPropagator.CapturedTaskContext captured =
        new TaskContextPropagator.CapturedTaskContext(realmContext, principal, "req-99");

    propagator.restore(captured);

    verify(realmHolder).set(realmContext);
    verify(principalHolder).set(principal);
    verify(requestIdHolder).set("req-99");
  }

  @Test
  void testCaptureAndRestoreRoundTrip() {
    RealmContext realmContext = () -> "round-trip-realm";
    PolarisPrincipal principal = PolarisPrincipal.of("eve", Map.of(), Set.of());

    // Source holders for capture
    RealmContextHolder sourceRealmHolder = mock(RealmContextHolder.class);
    when(sourceRealmHolder.get()).thenReturn(realmContext);
    RequestIdHolder sourceRequestIdHolder = mock(RequestIdHolder.class);
    when(sourceRequestIdHolder.get()).thenReturn("req-rt");

    TaskContextPropagator propagator =
        new TaskContextPropagator(
            sourceRealmHolder,
            mock(PolarisPrincipalHolder.class),
            sourceRequestIdHolder,
            principal);

    TaskContextPropagator.CapturedTaskContext captured = propagator.capture();

    // Target holders for restore (simulating task thread with fresh holders)
    RealmContextHolder targetRealmHolder = mock(RealmContextHolder.class);
    PolarisPrincipalHolder targetPrincipalHolder = mock(PolarisPrincipalHolder.class);
    RequestIdHolder targetRequestIdHolder = mock(RequestIdHolder.class);

    TaskContextPropagator targetPropagator =
        new TaskContextPropagator(
            targetRealmHolder, targetPrincipalHolder, targetRequestIdHolder, principal);

    targetPropagator.restore(captured);

    verify(targetRealmHolder).set(realmContext);
    verify(targetPrincipalHolder).set(captured.principal());
    verify(targetRequestIdHolder).set("req-rt");
  }

  @Test
  void testRestoreWithNullRequestId() {
    RealmContext realmContext = () -> "realm";
    PolarisPrincipal principal = PolarisPrincipal.of("frank", Map.of(), Set.of());

    RealmContextHolder realmHolder = mock(RealmContextHolder.class);
    PolarisPrincipalHolder principalHolder = mock(PolarisPrincipalHolder.class);
    RequestIdHolder requestIdHolder = mock(RequestIdHolder.class);

    TaskContextPropagator propagator =
        new TaskContextPropagator(realmHolder, principalHolder, requestIdHolder, principal);

    TaskContextPropagator.CapturedTaskContext captured =
        new TaskContextPropagator.CapturedTaskContext(realmContext, principal, null);

    propagator.restore(captured);

    verify(realmHolder).set(realmContext);
    verify(principalHolder).set(principal);
    verify(requestIdHolder).set(null);
  }
}
