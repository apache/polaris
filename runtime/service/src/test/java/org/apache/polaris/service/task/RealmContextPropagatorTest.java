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

import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RealmContextPropagator}. */
class RealmContextPropagatorTest {

  private RealmContextHolder holder;
  private RealmContextPropagator propagator;

  @BeforeEach
  void setUp() {
    holder = new RealmContextHolder();
    propagator = new RealmContextPropagator(holder);
  }

  @Test
  void capture_withRealmInHolder_returnsRealmContext() {
    RealmContext realmContext = () -> "test-realm";
    holder.set(realmContext);

    Object state = propagator.capture();
    assertThat(state).isSameAs(realmContext);
  }

  @Test
  void capture_withNullRealmInHolder_returnsNull() {
    Object state = propagator.capture();
    assertThat(state).isNull();
  }

  @Test
  void restore_setsRealmInHolder() throws Exception {
    RealmContextHolder targetHolder = new RealmContextHolder();
    RealmContextPropagator targetPropagator = new RealmContextPropagator(targetHolder);

    RealmContext realmContext = () -> "restored-realm";
    try (AutoCloseable scope = targetPropagator.restore(realmContext)) {
      assertThat(targetHolder.get()).isSameAs(realmContext);
      assertThat(targetHolder.get().getRealmIdentifier()).isEqualTo("restored-realm");
    }
  }

  @Test
  void restore_nullState_doesNotSetHolder() throws Exception {
    RealmContextHolder targetHolder = new RealmContextHolder();
    RealmContextPropagator targetPropagator = new RealmContextPropagator(targetHolder);

    try (AutoCloseable scope = targetPropagator.restore(null)) {
      assertThat(scope).isNotNull();
      assertThat(targetHolder.get()).isNull();
    }
  }

  @Test
  void restore_returnedCloseableIsNoOp() throws Exception {
    RealmContextHolder targetHolder = new RealmContextHolder();
    RealmContextPropagator targetPropagator = new RealmContextPropagator(targetHolder);

    AutoCloseable scope = targetPropagator.restore((RealmContext) () -> "realm");
    assertThat(scope).isNotNull();
    scope.close(); // no-op; should not throw
  }
}
