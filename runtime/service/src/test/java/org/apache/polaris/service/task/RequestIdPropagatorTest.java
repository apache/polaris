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

import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/** Unit tests for {@link RequestIdPropagator}. */
class RequestIdPropagatorTest {

  private RequestIdHolder holder;
  private RequestIdPropagator propagator;

  @BeforeEach
  void setUp() {
    holder = new RequestIdHolder();
    propagator = new RequestIdPropagator(holder);
    MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
  }

  @AfterEach
  void tearDown() {
    MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
  }

  @Test
  void restore_setsHolderAndMdc() throws Exception {
    try (AutoCloseable scope = propagator.restore("req-123")) {
      assertThat(holder.get()).isEqualTo("req-123");
      assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("req-123");
    }

    // After close, MDC should be cleared (no previous value).
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isNull();
  }

  @Test
  void restore_restoresPreviousMdcValueOnClose() throws Exception {
    MDC.put(RequestIdFilter.REQUEST_ID_KEY, "previous-id");

    try (AutoCloseable scope = propagator.restore("task-req-456")) {
      assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("task-req-456");
    }

    // Previous MDC value should be restored.
    assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isEqualTo("previous-id");

    MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
  }

  @Test
  void restore_nullRequestId_doesNotSetMdc() throws Exception {
    try (AutoCloseable scope = propagator.restore(null)) {
      assertThat(holder.get()).isNull();
      assertThat(MDC.get(RequestIdFilter.REQUEST_ID_KEY)).isNull();
    }
  }

  @Test
  void capture_withNoActiveJaxrsContext_usesRestoredHolderValue() {
    holder.set("nested-task-req");

    Object state = propagator.capture();
    assertThat(state).isEqualTo("nested-task-req");
  }

  @Test
  void capture_withNoActiveJaxrsContext_andNoRestoredHolder_returnsNull() {
    // No JAX-RS request active (CurrentRequestManager.get() returns null in unit tests).
    // Holder is also empty.
    Object state = propagator.capture();
    assertThat(state).isNull();
  }
}
