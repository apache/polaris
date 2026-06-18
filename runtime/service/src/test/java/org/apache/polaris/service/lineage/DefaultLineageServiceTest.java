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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageDirection;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageGranularity;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineageIngestRequest;
import org.apache.polaris.core.lineage.LineageNode;
import org.apache.polaris.core.lineage.LineageNodeType;
import org.apache.polaris.core.lineage.LineagePersistence;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultLineageServiceTest {
  @Mock private CallContext callContext;
  @Mock private RealmConfig realmConfig;
  @Mock private LineageConfiguration configuration;
  @Mock private LineagePersistence persistence;
  @Mock private RealmContext realmContext;

  private DefaultLineageService service;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(callContext.getRealmContext()).thenReturn(realmContext);
    service = new DefaultLineageService(callContext, configuration, persistence);
  }

  @Test
  void throwsWhenStaticConfigDisabled() {
    when(configuration.enabled()).thenReturn(false);

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.enabled");

    verifyNoInteractions(persistence);
  }

  @Test
  void throwsWhenRealmFeatureDisabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(false);

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.ENABLE_LINEAGE.key());

    verifyNoInteractions(persistence);
  }

  @Test
  void delegatesQueryWhenLineageEnabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(true);
    LineageQueryRequest request = queryRequest();
    LineageGraph graph =
        new LineageGraph(
            new LineageNode("dataset:test:orders", LineageNodeType.DATASET, null, false),
            List.of(),
            List.of());
    when(persistence.loadLineage(realmContext, request)).thenReturn(graph);

    assertThat(service.query(request)).isSameAs(graph);
    verify(persistence).loadLineage(realmContext, request);
  }

  @Test
  void delegatesIngestWhenLineageEnabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(true);
    LineageIngestRequest request = ingestRequest();

    service.ingest(request);

    Instant lastEventAt = Instant.parse("2026-01-01T00:00:00Z");
    InOrder inOrder = inOrder(persistence);
    inOrder.verify(persistence).upsertDatasets(realmContext, request.datasets());
    inOrder.verify(persistence).upsertDatasetEdges(realmContext, request.edges(), lastEventAt);
    inOrder.verify(persistence).upsertColumnEdges(realmContext, request.columnEdges(), lastEventAt);
  }

  private static LineageQueryRequest queryRequest() {
    return new LineageQueryRequest(
        "dataset:test:orders", LineageDirection.BOTH, LineageGranularity.DATASET);
  }

  private static LineageIngestRequest ingestRequest() {
    LineageDataset source = new LineageDataset("test", "analytics", "orders_raw");
    LineageDataset target = new LineageDataset("test", "analytics", "orders_daily");
    return new LineageIngestRequest(
        List.of(source, target),
        List.of(new LineageEdge(source, target)),
        List.of(),
        Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
  }
}
