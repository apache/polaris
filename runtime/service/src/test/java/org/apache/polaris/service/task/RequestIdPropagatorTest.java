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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/** Unit tests for {@link RequestIdPropagator}. */
class RequestIdPropagatorTest {

  @BeforeEach
  void setUp() {
    MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
  }

  @AfterEach
  void tearDown() {
    MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
  }

  @Test
  void testRestoreSetsHolderAndMdc() throws Exception {
    // Use a mock to simulate CDI proxy behavior across request scopes.
    RequestIdHolder mockHolder = mock(RequestIdHolder.class);
    when(mockHolder.get()).thenReturn("req-123");

    RequestIdPropagator propagator = new RequestIdPropagator(mockHolder);
    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();

    action.restore();
    verify(mockHolder).set("req-123");
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("req-123");

    action.close();
    // After close, MDC should be cleared (no previous value).
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isNull();
  }

  @Test
  void testRestoreRestoresPreviousMdcValueOnClose() throws Exception {
    MDC.put(RequestIdFilter.REQUEST_ID_KEY, "previous-id");

    RequestIdHolder mockHolder = mock(RequestIdHolder.class);
    when(mockHolder.get()).thenReturn("task-req-456");

    RequestIdPropagator propagator = new RequestIdPropagator(mockHolder);
    AsyncContextPropagator.RestoreAction action = propagator.capture();
    assertThat(action).isNotNull();

    action.restore();
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("task-req-456");

    action.close();
    // Previous MDC value should be restored.
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("previous-id");
  }

  @Test
  void testCaptureWithNullRequestIdReturnsNoop() {
    RequestIdHolder holder = new RequestIdHolder();
    RequestIdPropagator propagator = new RequestIdPropagator(holder);
    assertThat(propagator.capture()).isSameAs(AsyncContextPropagator.RestoreAction.NOOP);
  }

  @Test
  void testCaptureUsesHolderValue() {
    RequestIdHolder holder = new RequestIdHolder();
    holder.set("nested-task-req");
    RequestIdPropagator propagator = new RequestIdPropagator(holder);
    assertThat(propagator.capture()).isNotNull();
  }

  @Test
  void testHolderDoubleSetThrowsIllegalStateException() {
    RequestIdHolder holder = new RequestIdHolder();
    holder.set("first");
    assertThrows(IllegalStateException.class, () -> holder.set("second"));
  }
}
