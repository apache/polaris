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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
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

/**
 * Conditional GET (304 notModified) has no response entity; AFTER_LOAD_TABLE must not store a null
 * LOAD_TABLE_RESPONSE.
 */
public class IcebergRestCatalogEventServiceDelegatorLoadTableTest {

  private IcebergCatalogAdapter delegate;
  private PolarisEventDispatcher eventDispatcher;
  private PolarisEventMetadataFactory eventMetadataFactory;
  private CatalogPrefixParser prefixParser;
  private IcebergRestCatalogEventServiceDelegator delegator;

  @BeforeEach
  void setUp() {
    delegate = mock(IcebergCatalogAdapter.class);
    eventDispatcher = mock(PolarisEventDispatcher.class);
    eventMetadataFactory = mock(PolarisEventMetadataFactory.class);
    prefixParser = mock(CatalogPrefixParser.class);
    when(prefixParser.prefixToCatalogName(anyString())).thenReturn("test-catalog");
    when(eventMetadataFactory.create())
        .thenReturn(PolarisEventMetadata.builder().realmId("test-realm").build());
    when(eventDispatcher.hasListeners(any())).thenReturn(false);
    when(eventDispatcher.hasListeners(PolarisEventType.AFTER_LOAD_TABLE)).thenReturn(true);

    delegator =
        new IcebergRestCatalogEventServiceDelegator(
            delegate,
            eventDispatcher,
            eventMetadataFactory,
            prefixParser,
            new MutableAttributeMap());
  }

  @Test
  void afterLoadTableOmitsResponseWhenNotModified() {
    when(delegate.loadTable(
            anyString(),
            anyString(),
            anyString(),
            any(),
            any(),
            any(),
            any(),
            any(RealmContext.class),
            any(SecurityContext.class)))
        .thenReturn(Response.notModified().build());

    Response response =
        delegator.loadTable(
            "prefix",
            "ns",
            "t1",
            null,
            "W/\"etag\"",
            null,
            null,
            mock(RealmContext.class),
            mock(SecurityContext.class));

    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
    assertThat(response.getEntity()).isNull();

    ArgumentCaptor<PolarisEvent> eventCaptor = ArgumentCaptor.forClass(PolarisEvent.class);
    verify(eventDispatcher).dispatch(eventCaptor.capture());
    PolarisEvent afterEvent = eventCaptor.getValue();
    assertThat(afterEvent.type()).isEqualTo(PolarisEventType.AFTER_LOAD_TABLE);
    assertThat(afterEvent.attributes().containsKey(EventAttributes.LOAD_TABLE_RESPONSE)).isFalse();
    assertThat(afterEvent.attributes().get(EventAttributes.TABLE_NAME)).isEqualTo("t1");
  }

  @Test
  void afterLoadTableIncludesResponseWhenEntityPresent() {
    TableMetadata metadata =
        TableMetadata.buildFromEmpty()
            .assignUUID()
            .setLocation("file:///tmp/t1")
            .addSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();
    LoadTableResponse loadTableResponse =
        LoadTableResponse.builder().withTableMetadata(metadata).build();
    when(delegate.loadTable(
            anyString(),
            anyString(),
            anyString(),
            any(),
            any(),
            any(),
            any(),
            any(RealmContext.class),
            any(SecurityContext.class)))
        .thenReturn(Response.ok(loadTableResponse).build());

    delegator.loadTable(
        "prefix",
        "ns",
        "t1",
        null,
        null,
        null,
        null,
        mock(RealmContext.class),
        mock(SecurityContext.class));

    ArgumentCaptor<PolarisEvent> eventCaptor = ArgumentCaptor.forClass(PolarisEvent.class);
    verify(eventDispatcher).dispatch(eventCaptor.capture());
    assertThat(eventCaptor.getValue().attributes().get(EventAttributes.LOAD_TABLE_RESPONSE))
        .isSameAs(loadTableResponse);
  }
}
