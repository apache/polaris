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
package org.apache.polaris.service.lineage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.lineage.api.OpenLineageBatchIngestResponse;
import org.apache.polaris.service.lineage.api.OpenLineageIngestProvider;
import org.apache.polaris.service.lineage.api.OpenLineageIngestRequest;
import org.apache.polaris.service.lineage.api.OpenLineageIngestResult;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link OpenLineageAdapter}, which maps provider {@link OpenLineageIngestResult}s
 * to HTTP responses and aggregates a batch of per-event outcomes into an {@link
 * OpenLineageBatchIngestResponse}.
 *
 * <p>The provider is mocked so every outcome branch — including {@code REJECTED} and {@code
 * UNAVAILABLE} — can be exercised. The full-server {@code OpenLineageServiceIT} only ever sees the
 * no-op provider's {@code ACCEPTED}, so the partial/failure aggregation and the non-201 status
 * mappings have no other coverage.
 */
class OpenLineageAdapterTest {

  private OpenLineageIngestProvider provider;
  private RealmConfig realmConfig;
  private OpenLineageAdapter adapter;
  private RealmContext realmContext;

  @BeforeEach
  void setUp() {
    provider = mock(OpenLineageIngestProvider.class);
    realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(true);
    adapter = new OpenLineageAdapter(provider, realmConfig);
    realmContext = mock(RealmContext.class);
    when(realmContext.getRealmIdentifier()).thenReturn("test-realm");
  }

  // The underlying OpenLineage event is irrelevant to response mapping / aggregation (the provider
  // is mocked), so a wrapper around a null event keeps these tests focused on the adapter logic.
  private static PolarisLineageEvent event() {
    return new PolarisLineageEvent.OfRunEvent(null);
  }

  @Test
  void acceptedMapsTo201() {
    when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
    Response response = adapter.sendLineageEvent(event(), realmContext, null);
    assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
  }

  @Test
  void rejectedMapsTo400() {
    when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.REJECTED);
    Response response = adapter.sendLineageEvent(event(), realmContext, null);
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  void unavailableMapsTo503() {
    when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.UNAVAILABLE);
    Response response = adapter.sendLineageEvent(event(), realmContext, null);
    assertThat(response.getStatus()).isEqualTo(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  void realmIdentifierIsPassedToProvider() {
    when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
    adapter.sendLineageEvent(event(), realmContext, null);

    ArgumentCaptor<OpenLineageIngestRequest> captor =
        ArgumentCaptor.forClass(OpenLineageIngestRequest.class);
    verify(provider).ingest(captor.capture());
    assertThat(captor.getValue().realmId()).isEqualTo("test-realm");
  }

  @Test
  void batchAllAcceptedIsSuccess() {
    when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);

    Response response =
        adapter.sendLineageEventBatch(List.of(event(), event(), event()), realmContext, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    OpenLineageBatchIngestResponse body = (OpenLineageBatchIngestResponse) response.getEntity();
    assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
    assertThat(body.summary().received()).isEqualTo(3);
    assertThat(body.summary().successful()).isEqualTo(3);
    assertThat(body.summary().failed()).isZero();
    assertThat(body.failedEvents()).isEmpty();
  }

  @Test
  void batchMixedOutcomesIsPartialWithPerEventDetail() {
    // Event 0 accepted, event 1 rejected (not retriable), event 2 backend unavailable (retriable).
    when(provider.ingest(any()))
        .thenReturn(
            OpenLineageIngestResult.ACCEPTED,
            OpenLineageIngestResult.REJECTED,
            OpenLineageIngestResult.UNAVAILABLE);

    Response response =
        adapter.sendLineageEventBatch(List.of(event(), event(), event()), realmContext, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    OpenLineageBatchIngestResponse body = (OpenLineageBatchIngestResponse) response.getEntity();
    assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.PARTIAL);
    assertThat(body.summary().received()).isEqualTo(3);
    assertThat(body.summary().successful()).isEqualTo(1);
    assertThat(body.summary().failed()).isEqualTo(2);

    assertThat(body.failedEvents()).hasSize(2);
    OpenLineageBatchIngestResponse.FailedEvent rejected = body.failedEvents().get(0);
    assertThat(rejected.index()).isEqualTo(1);
    assertThat(rejected.retriable()).isFalse();
    assertThat(rejected.message()).isEqualTo("Event rejected");

    OpenLineageBatchIngestResponse.FailedEvent unavailable = body.failedEvents().get(1);
    assertThat(unavailable.index()).isEqualTo(2);
    assertThat(unavailable.retriable()).isTrue();
    assertThat(unavailable.message()).isEqualTo("Ingest backend unavailable");
  }

  @Test
  void batchAllFailedIsFailure() {
    when(provider.ingest(any()))
        .thenReturn(OpenLineageIngestResult.REJECTED, OpenLineageIngestResult.UNAVAILABLE);

    Response response =
        adapter.sendLineageEventBatch(List.of(event(), event()), realmContext, null);

    OpenLineageBatchIngestResponse body = (OpenLineageBatchIngestResponse) response.getEntity();
    assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.FAILURE);
    assertThat(body.summary().received()).isEqualTo(2);
    assertThat(body.summary().successful()).isZero();
    assertThat(body.summary().failed()).isEqualTo(2);
    assertThat(body.failedEvents()).hasSize(2);
  }

  @Test
  void emptyBatchIsSuccessAndDoesNotCallProvider() {
    Response response = adapter.sendLineageEventBatch(List.of(), realmContext, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    OpenLineageBatchIngestResponse body = (OpenLineageBatchIngestResponse) response.getEntity();
    assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
    assertThat(body.summary().received()).isZero();
    assertThat(body.summary().successful()).isZero();
    assertThat(body.summary().failed()).isZero();
    assertThat(body.failedEvents()).isEmpty();
    verify(provider, never()).ingest(any());
  }

  @Test
  void disabledFlagReturns404AndDoesNotCallProvider() {
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(false);

    Response response = adapter.sendLineageEvent(event(), realmContext, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    verify(provider, never()).ingest(any());
  }

  @Test
  void disabledFlagBatchReturns404AndDoesNotCallProvider() {
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(false);

    Response response =
        adapter.sendLineageEventBatch(List.of(event(), event()), realmContext, null);

    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    verify(provider, never()).ingest(any());
  }
}
