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
package org.apache.polaris.persistence.nosql.cdi.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRealmPersistence {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void backendWrapperPreservesInjectedBackend(boolean skipDecorators) {
    var injectedBackend = mock(Backend.class);
    var concreteBackend = mock(Backend.class);
    var wrappedBackend = mock(Backend.class);
    var persistence = mock(Persistence.class);
    var decoratedPersistence = mock(Persistence.class);
    var params = mock(PersistenceParams.class);
    var clock = mock(MonotonicClock.class);
    var idGenerator = mock(IdGenerator.class);
    var decorators = mock(PersistenceDecorators.class);
    var calls = new AtomicInteger();

    when(injectedBackend.newPersistence(any(), eq(params), eq("realm"), eq(clock), eq(idGenerator)))
        .thenAnswer(invocation -> {
          Function<Backend, Backend> wrapper = invocation.getArgument(0);
          assertThat(wrapper.apply(concreteBackend)).isSameAs(wrappedBackend);
          return persistence;
        });
    when(decorators.decorate(persistence)).thenReturn(decoratedPersistence);

    var builder =
        new RealmPersistence(params, injectedBackend, idGenerator, clock, decorators)
            .newBuilder()
            .realmId("realm")
            .backendWrapper(backend -> {
              assertThat(backend).isSameAs(injectedBackend);
              calls.incrementAndGet();
              return wrappedBackend;
            });
    if (skipDecorators) {
      builder.skipDecorators();
    }
    assertThat(builder.build()).isSameAs(skipDecorators ? persistence : decoratedPersistence);
    assertThat(calls).hasValue(1);
    assertThatIllegalStateException().isThrownBy(builder::build);
    assertThatIllegalStateException().isThrownBy(() -> builder.backendWrapper(Function.identity()));
    if (skipDecorators) {
      verifyNoInteractions(decorators);
    } else {
      verify(decorators).decorate(persistence);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void defaultWrapperUsesInjectedBackend(boolean skipDecorators) {
    var backend = mock(Backend.class);
    var concreteBackend = mock(Backend.class);
    var persistence = mock(Persistence.class);
    var params = mock(PersistenceParams.class);
    var clock = mock(MonotonicClock.class);
    var ids = mock(IdGenerator.class);
    var decorators = mock(PersistenceDecorators.class);
    when(backend.newPersistence(any(), eq(params), eq("realm"), eq(clock), eq(ids)))
        .thenAnswer(invocation -> {
          Function<Backend, Backend> wrapper = invocation.getArgument(0);
          assertThat(wrapper.apply(concreteBackend)).isSameAs(backend);
          return persistence;
        });
    when(decorators.decorate(persistence)).thenReturn(persistence);
    var builder =
        new RealmPersistence(params, backend, ids, clock, decorators).newBuilder().realmId("realm");
    if (skipDecorators) {
      builder.skipDecorators();
    }
    assertThat(builder.build()).isSameAs(persistence);
  }
}
