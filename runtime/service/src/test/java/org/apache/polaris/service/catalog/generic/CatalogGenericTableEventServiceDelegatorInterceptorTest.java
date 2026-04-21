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

package org.apache.polaris.service.catalog.generic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.Response;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventInterceptor;
import org.apache.polaris.service.events.PolarisEventInterceptorManager;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class CatalogGenericTableEventServiceDelegatorInterceptorTest {

  private CatalogGenericTableEventServiceDelegator delegator;
  private GenericTableCatalogAdapter delegate;
  private PolarisEventDispatcher eventDispatcher;

  @BeforeEach
  void setUp() {
    delegator = new CatalogGenericTableEventServiceDelegator();

    delegate = mock(GenericTableCatalogAdapter.class);
    eventDispatcher = mock(PolarisEventDispatcher.class);
    PolarisEventMetadataFactory metadataFactory = mock(PolarisEventMetadataFactory.class);
    CatalogPrefixParser prefixParser = mock(CatalogPrefixParser.class);

    when(prefixParser.prefixToCatalogName("cat")).thenReturn("cat");
    when(metadataFactory.create()).thenReturn(null);

    delegator.delegate = delegate;
    delegator.polarisEventDispatcher = eventDispatcher;
    delegator.polarisEventInterceptorManager =
        new PolarisEventInterceptorManager(java.util.List.of());
    delegator.eventMetadataFactory = metadataFactory;
    delegator.prefixParser = prefixParser;
  }

  @Test
  void denyAbortsOperationBeforeDelegateInvocation() {
    PolarisEventInterceptor denyInterceptor =
        event -> PolarisEventInterceptor.Result.deny("drop blocked by policy");
    delegator.polarisEventInterceptorManager =
        new PolarisEventInterceptorManager(java.util.List.of(denyInterceptor));

    assertThatThrownBy(() -> delegator.dropGenericTable("cat", "ns", "orders", null, null))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("drop blocked by policy");

    verify(delegate, never()).dropGenericTable(any(), any(), any(), any(), any());
    verify(eventDispatcher, never()).dispatch(any());
  }

  @Test
  void allowDispatchesEventsAndCallsDelegate() {
    PolarisEventInterceptor allowInterceptor = event -> PolarisEventInterceptor.Result.allow();
    delegator.polarisEventInterceptorManager =
        new PolarisEventInterceptorManager(java.util.List.of(allowInterceptor));
    when(eventDispatcher.hasListeners(PolarisEventType.BEFORE_DROP_GENERIC_TABLE)).thenReturn(true);
    when(eventDispatcher.hasListeners(PolarisEventType.AFTER_DROP_GENERIC_TABLE)).thenReturn(true);

    when(delegate.dropGenericTable(any(), any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());

    Response response = delegator.dropGenericTable("cat", "ns", "orders", null, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    verify(delegate, times(1)).dropGenericTable(eq("cat"), eq("ns"), eq("orders"), any(), any());

    ArgumentCaptor<PolarisEvent> eventCaptor = ArgumentCaptor.forClass(PolarisEvent.class);
    verify(eventDispatcher, times(2)).dispatch(eventCaptor.capture());
    assertThat(eventCaptor.getAllValues().get(0).type())
        .isEqualTo(PolarisEventType.BEFORE_DROP_GENERIC_TABLE);
    assertThat(eventCaptor.getAllValues().get(1).type())
        .isEqualTo(PolarisEventType.AFTER_DROP_GENERIC_TABLE);
  }
}
