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
package org.apache.polaris.service.catalog.tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Pins the attribute shape of the tag read events: an optional query parameter that the request did
 * not carry is present in the event attributes with a null value, the same shape the policy read
 * events use, so listeners see one convention across both read families.
 */
public class CatalogTagEventServiceDelegatorTest {

  private CatalogTagEventServiceDelegator delegatorWithListeners(
      PolarisEventDispatcher dispatcher) {
    CatalogTagEventServiceDelegator delegator = new CatalogTagEventServiceDelegator();
    delegator.delegate = mock(TagCatalogAdapter.class);
    when(delegator.delegate.getObjectTags(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Response.ok().build());
    when(delegator.delegate.listObjectsByTag(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Response.ok().build());
    delegator.polarisEventDispatcher = dispatcher;
    delegator.eventMetadataFactory = mock(PolarisEventMetadataFactory.class);
    when(delegator.eventMetadataFactory.create()).thenReturn(mock(PolarisEventMetadata.class));
    delegator.prefixParser = mock(CatalogPrefixParser.class);
    when(delegator.prefixParser.prefixToCatalogName("prefix")).thenReturn("cat");
    return delegator;
  }

  @Test
  public void testGetObjectTagsEventsCarryAbsentParametersAsNullValues() {
    PolarisEventDispatcher dispatcher = mock(PolarisEventDispatcher.class);
    when(dispatcher.hasListeners(any(PolarisEventType.class))).thenReturn(true);
    CatalogTagEventServiceDelegator delegator = delegatorWithListeners(dispatcher);

    delegator.getObjectTags(
        "prefix", TargetType.NAMESPACE, null, "ns", null, null, null, null, null, null, null);

    ArgumentCaptor<PolarisEvent> events = ArgumentCaptor.forClass(PolarisEvent.class);
    verify(dispatcher, org.mockito.Mockito.times(2)).dispatch(events.capture());
    List<PolarisEvent> dispatched = events.getAllValues();
    assertThat(dispatched)
        .extracting(PolarisEvent::type)
        .containsExactly(
            PolarisEventType.BEFORE_GET_OBJECT_TAGS, PolarisEventType.AFTER_GET_OBJECT_TAGS);
    for (PolarisEvent event : dispatched) {
      assertThat(event.attributes().getRequired(EventAttributes.CATALOG_NAME)).isEqualTo("cat");
      assertThat(event.attributes().getRequired(EventAttributes.NAMESPACE_NAME)).isEqualTo("ns");
      // absent parameters are present keys with null values, matching the policy read events
      assertThat(event.attributes().containsKey(EventAttributes.TARGET_NAME)).isTrue();
      assertThat(event.attributes().containsKey(EventAttributes.COLUMN_NAME)).isTrue();
      assertThat(event.attributes().containsKey(EventAttributes.TAG_VIEW)).isTrue();
      assertThat(event.attributes().getOptional(EventAttributes.TARGET_NAME)).isEmpty();
      assertThat(event.attributes().getOptional(EventAttributes.TAG_VIEW)).isEmpty();
    }
  }

  @Test
  public void testListObjectsByTagEventsCarryAbsentValueFilterAsNull() {
    PolarisEventDispatcher dispatcher = mock(PolarisEventDispatcher.class);
    when(dispatcher.hasListeners(any(PolarisEventType.class))).thenReturn(true);
    CatalogTagEventServiceDelegator delegator = delegatorWithListeners(dispatcher);

    delegator.listObjectsByTag("prefix", "t1", null, null, null, null, null, null);

    ArgumentCaptor<PolarisEvent> events = ArgumentCaptor.forClass(PolarisEvent.class);
    verify(dispatcher, org.mockito.Mockito.times(2)).dispatch(events.capture());
    for (PolarisEvent event : events.getAllValues()) {
      assertThat(event.attributes().containsKey(EventAttributes.TAG_VALUE_FILTER)).isTrue();
      assertThat(event.attributes().getOptional(EventAttributes.TAG_VALUE_FILTER)).isEmpty();
    }
  }
}
