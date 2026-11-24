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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.collection.MutableAttributeMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class IcebergRestCatalogEventServiceDelegatorUnregisterTableTest {

  private IcebergCatalogAdapter delegate;
  private PolarisEventDispatcher eventDispatcher;
  private IcebergRestCatalogEventServiceDelegator delegator;
  private final RealmContext realmContext = mock(RealmContext.class);
  private final SecurityContext securityContext = mock(SecurityContext.class);
  private final UUID idempotencyKey = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    delegate = mock(IcebergCatalogAdapter.class);
    eventDispatcher = mock(PolarisEventDispatcher.class);
    PolarisEventMetadataFactory eventMetadataFactory = mock(PolarisEventMetadataFactory.class);
    CatalogPrefixParser prefixParser = mock(CatalogPrefixParser.class);
    when(prefixParser.prefixToCatalogName(anyString())).thenReturn("test-catalog");
    when(eventMetadataFactory.create())
        .thenReturn(PolarisEventMetadata.builder().realmId("test-realm").build());
    when(eventDispatcher.hasListeners(PolarisEventType.BEFORE_DROP_TABLE)).thenReturn(true);
    when(eventDispatcher.hasListeners(PolarisEventType.AFTER_DROP_TABLE)).thenReturn(true);
    delegator =
        new IcebergRestCatalogEventServiceDelegator(
            delegate,
            eventDispatcher,
            eventMetadataFactory,
            prefixParser,
            new MutableAttributeMap());
  }

  @Test
  void unregisterEmitsDropEventsWithoutPurge() {
    Response expected = Response.ok().build();
    when(delegate.unregisterTable(
            "prefix", "ns", "table", idempotencyKey, realmContext, securityContext))
        .thenReturn(expected);

    assertThat(
            delegator.unregisterTable(
                "prefix", "ns", "table", idempotencyKey, realmContext, securityContext))
        .isSameAs(expected);

    ArgumentCaptor<PolarisEvent> events = ArgumentCaptor.forClass(PolarisEvent.class);
    InOrder order = inOrder(eventDispatcher, delegate);
    order.verify(eventDispatcher).dispatch(events.capture());
    order
        .verify(delegate)
        .unregisterTable("prefix", "ns", "table", idempotencyKey, realmContext, securityContext);
    order.verify(eventDispatcher).dispatch(events.capture());
    assertThat(events.getAllValues())
        .extracting(PolarisEvent::type)
        .containsExactly(PolarisEventType.BEFORE_DROP_TABLE, PolarisEventType.AFTER_DROP_TABLE);
    assertThat(events.getAllValues())
        .allSatisfy(event -> {
          assertThat(event.attributes().get(EventAttributes.PURGE_REQUESTED)).isFalse();
          assertThat(event.attributes().get(EventAttributes.CATALOG_NAME))
              .isEqualTo("test-catalog");
          assertThat(event.attributes().get(EventAttributes.NAMESPACE))
              .isEqualTo(Namespace.of("ns"));
          assertThat(event.attributes().get(EventAttributes.TABLE_NAME)).isEqualTo("table");
        });
  }

  @Test
  void unregisterFailureOmitsAfterDropEvent() {
    RuntimeException failure = new IllegalStateException("Unregister failed");
    when(delegate.unregisterTable(
            "prefix", "ns", "table", idempotencyKey, realmContext, securityContext))
        .thenThrow(failure);

    assertThatThrownBy(() ->
            delegator.unregisterTable(
                "prefix", "ns", "table", idempotencyKey, realmContext, securityContext))
        .isSameAs(failure);

    ArgumentCaptor<PolarisEvent> events = ArgumentCaptor.forClass(PolarisEvent.class);
    InOrder order = inOrder(eventDispatcher, delegate);
    order.verify(eventDispatcher).dispatch(events.capture());
    order
        .verify(delegate)
        .unregisterTable("prefix", "ns", "table", idempotencyKey, realmContext, securityContext);
    order.verifyNoMoreInteractions();
    assertThat(events.getAllValues())
        .singleElement()
        .satisfies(event -> {
          assertThat(event.type()).isEqualTo(PolarisEventType.BEFORE_DROP_TABLE);
          assertThat(event.attributes().get(EventAttributes.PURGE_REQUESTED)).isFalse();
        });
  }
}
