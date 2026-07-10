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
package org.apache.polaris.service.catalog.semanticmodel;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.junit.jupiter.api.Test;

public class SemanticModelConfigEndpointsTest {
  @Test
  public void endpointsIncludeSemanticModelsWhenFeatureEnabled() {
    SemanticModelConfigEndpoints endpoints = new SemanticModelConfigEndpoints(realmConfig(true));

    assertThat(endpoints.endpoints())
        .containsExactlyInAnyOrderElementsOf(SemanticModelEndpoints.SEMANTIC_MODEL_ENDPOINTS);
  }

  @Test
  public void endpointsEmptyWhenFeatureDisabled() {
    SemanticModelConfigEndpoints endpoints = new SemanticModelConfigEndpoints(realmConfig(false));

    assertThat(endpoints.endpoints()).isEmpty();
  }

  private static RealmConfig realmConfig(boolean enableSemanticModels) {
    return new RealmConfigImpl(
        (realmContext, configName) ->
            FeatureConfiguration.ENABLE_SEMANTIC_MODELS.key().equals(configName)
                ? enableSemanticModels
                : null,
        () -> "test-realm");
  }
}
