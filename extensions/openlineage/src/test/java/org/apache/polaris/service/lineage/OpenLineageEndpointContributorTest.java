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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OpenLineageEndpointContributor}, which advertises the OpenLineage ingest
 * endpoints only when {@link FeatureConfiguration#ENABLE_OPENLINEAGE_INGEST} is enabled.
 *
 * <p>The full-server {@code OpenLineageServiceIT} only runs with the flag on, so the disabled path
 * (advertise nothing) has no other coverage.
 */
class OpenLineageEndpointContributorTest {

  @Test
  void advertisesEndpointsWhenFlagEnabled() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(true);

    OpenLineageEndpointContributor contributor = new OpenLineageEndpointContributor(realmConfig);

    assertThat(contributor.endpoints())
        .isEqualTo(OpenLineageEndpoints.OPENLINEAGE_ENDPOINTS)
        .contains(
            OpenLineageEndpoints.V1_SEND_LINEAGE_EVENT,
            OpenLineageEndpoints.V1_SEND_LINEAGE_EVENT_BATCH);
  }

  @Test
  void advertisesNothingWhenFlagDisabled() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(false);

    OpenLineageEndpointContributor contributor = new OpenLineageEndpointContributor(realmConfig);

    assertThat(contributor.endpoints()).isEmpty();
  }
}
