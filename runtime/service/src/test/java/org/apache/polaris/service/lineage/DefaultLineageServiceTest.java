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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageColumnEdge;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageFieldReference;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineageIngestRequest;
import org.apache.polaris.core.lineage.LineageNode;
import org.apache.polaris.core.lineage.LineageNodeType;
import org.apache.polaris.core.lineage.LineagePersistence;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultLineageServiceTest {
  private static final Instant EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

  @Mock private CallContext callContext;
  @Mock private RealmContext realmContext;
  @Mock private RealmConfig realmConfig;
  @Mock private LineageConfiguration configuration;
  @Mock private LineageConfiguration.PersistenceConfiguration persistenceConfiguration;
  @Mock private LineagePersistence persistence;

  private DefaultLineageService service;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(callContext.getRealmContext()).thenReturn(realmContext);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(configuration.persistence()).thenReturn(persistenceConfiguration);
    service = new DefaultLineageService(callContext, configuration, persistence);
  }

  @Test
  void throwsWhenStaticConfigDisabled() {
    when(configuration.enabled()).thenReturn(false);

    assertThatThrownBy(() -> service.ingest(emptyIngestRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.enabled");

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.enabled");

    verifyNoInteractions(persistence);
  }

  @Test
  void throwsWhenRealmFeatureDisabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(false);

    assertThatThrownBy(() -> service.ingest(emptyIngestRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.ENABLE_LINEAGE.key());

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.ENABLE_LINEAGE.key());

    verifyNoInteractions(persistence);
  }

  @Test
  void throwsWhenPersistenceDisabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(true);
    when(persistenceConfiguration.enabled()).thenReturn(false);

    assertThatThrownBy(() -> service.ingest(emptyIngestRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.persistence.enabled");

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.persistence.enabled");

    verifyNoInteractions(persistence);
  }

  @Test
  void delegatesWhenLineageEnabled() {
    LineageIngestRequest ingestRequest = ingestRequest();
    LineageGraph graph =
        new LineageGraph(
            new LineageNode(
                "dataset:test:orders", LineageNodeType.DATASET, dataset("test", "orders"), false),
            List.of(),
            List.of());

    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(true);
    when(persistenceConfiguration.enabled()).thenReturn(true);
    when(persistence.loadLineage(realmContext, queryRequest())).thenReturn(graph);

    service.ingest(ingestRequest);
    service.query(queryRequest());

    verify(persistence).upsertDatasets(realmContext, ingestRequest.datasets());
    verify(persistence).upsertDatasetEdges(realmContext, ingestRequest.edges(), EVENT_TIME);
    verify(persistence).upsertColumnEdges(realmContext, ingestRequest.columnEdges(), EVENT_TIME);
    verify(persistence).loadLineage(realmContext, queryRequest());
  }

  private static LineageIngestRequest emptyIngestRequest() {
    return new LineageIngestRequest(List.of(), List.of(), List.of(), Optional.empty());
  }

  private static LineageIngestRequest ingestRequest() {
    LineageDataset source = dataset("raw", "orders");
    LineageDataset target = dataset("test", "orders");
    return new LineageIngestRequest(
        List.of(source, target),
        List.of(new LineageEdge(source, target)),
        List.of(
            new LineageColumnEdge(
                new LineageFieldReference(source, "id"),
                new LineageFieldReference(target, "order_id"))),
        Optional.of(EVENT_TIME));
  }

  private static LineageQueryRequest queryRequest() {
    return new LineageQueryRequest(
        "dataset:test:orders",
        org.apache.polaris.core.lineage.LineageDirection.BOTH,
        org.apache.polaris.core.lineage.LineageGranularity.DATASET);
  }

  private static LineageDataset dataset(String namespace, String name) {
    return new LineageDataset("test-catalog", namespace, name, OptionalLong.empty());
  }
}
