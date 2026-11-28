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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.junit.jupiter.api.Test;

class TestThrottledBackend {
  private final Backend delegate = mock(Backend.class);
  private final RateLimit objects = mock(RateLimit.class);
  private final RateLimit references = mock(RateLimit.class);
  private final ThrottledBackend backend = new ThrottledBackend(delegate, objects, references);

  @Test
  void referenceReadAcquiresBeforeFetching() {
    var ref = mock(Reference.class);
    when(delegate.fetchReference("realm", "ref")).thenReturn(ref);
    assertThat(backend.fetchReference("realm", "ref")).isSameAs(ref);
    var order = inOrder(references, delegate);
    order.verify(references).acquire();
    order.verify(delegate).fetchReference("realm", "ref");
    verifyNoInteractions(objects);
  }

  @Test
  void objectReadsChargeAllRequestedPartsIncludingMissingOnes() {
    var ids = Set.of(persistId(1, 0), persistId(1, 1), persistId(2, 0));
    var result = Map.of(persistId(1, 0), mock(FetchedObj.class));
    when(delegate.fetch("realm", ids)).thenReturn(result);
    assertThat(backend.fetch("realm", ids)).isSameAs(result);
    var order = inOrder(objects, delegate);
    order.verify(objects).acquire(3);
    order.verify(delegate).fetch("realm", ids);
    verifyNoInteractions(references);
  }

  @Test
  void emptyFetchDoesNotAcquire() {
    backend.fetch("realm", Set.of());
    verifyNoInteractions(objects, references);
    verify(delegate).fetch("realm", Set.of());
  }

  @Test
  void failedAdmissionPreventsRead() {
    var failure = new IllegalStateException("admission failed");
    doThrow(failure).when(objects).acquire(1);
    assertThatThrownBy(() -> backend.fetch("realm", Set.of(persistId(1, 0)))).isSameAs(failure);
    verifyNoInteractions(delegate, references);
  }

  @Test
  void closeDoesNotCloseSharedBackend() throws Exception {
    backend.close();
    verifyNoInteractions(delegate);
  }
}
