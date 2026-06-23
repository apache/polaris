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
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.lineage.LineageDirection;
import org.apache.polaris.lineage.LineageGranularity;
import org.apache.polaris.lineage.LineageQueryRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultLineageServiceTest {
  @Mock private CallContext callContext;
  @Mock private RealmConfig realmConfig;
  @Mock private LineageConfiguration configuration;

  private DefaultLineageService service;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    service = new DefaultLineageService(callContext, configuration);
  }

  @Test
  void throwsWhenStaticConfigDisabled() {
    when(configuration.enabled()).thenReturn(false);

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("polaris.lineage.enabled");
  }

  @Test
  void throwsWhenRealmFeatureDisabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(false);

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.ENABLE_LINEAGE.key());
  }

  @Test
  void throwsNotImplementedWhenLineageEnabled() {
    when(configuration.enabled()).thenReturn(true);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_LINEAGE)).thenReturn(true);

    assertThatThrownBy(() -> service.query(queryRequest()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Lineage query is not implemented yet");
  }

  private static LineageQueryRequest queryRequest() {
    return new LineageQueryRequest(
        "dataset:test:orders", LineageDirection.BOTH, LineageGranularity.DATASET);
  }
}
