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
package org.apache.polaris.persistence.nosql.quarkus.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.junit.jupiter.api.Test;

public class TestBackendProvider {
  @SuppressWarnings("unchecked")
  @Test
  public void backendProducerReturnsInitializedBackendInstance() throws Exception {
    var backendConfiguration = mock(BackendConfiguration.class);
    when(backendConfiguration.backend()).thenReturn(Optional.of("in-memory"));

    var backendBuilders = mock(Instance.class);
    var selectedBuilder = mock(Instance.class);
    var backendBuilder = mock(BackendBuilder.class);
    var backend = mock(Backend.class);

    when(backendBuilders.select(any(BackendType.class))).thenReturn(selectedBuilder);
    when(selectedBuilder.isResolvable()).thenReturn(true);
    when(selectedBuilder.isAmbiguous()).thenReturn(false);
    when(selectedBuilder.get()).thenReturn(backendBuilder);
    when(backendBuilder.buildBackend()).thenReturn(backend);
    when(backend.setupSchema()).thenReturn(Optional.of("created"));
    when(backend.type()).thenReturn("in-memory");

    var result = new BackendProvider().backend(backendConfiguration, backendBuilders);

    assertThat(result).isSameAs(backend);
    verify(backendBuilder).buildBackend();
    verify(backend).setupSchema();
    verify(backend, never()).close();
  }
}
