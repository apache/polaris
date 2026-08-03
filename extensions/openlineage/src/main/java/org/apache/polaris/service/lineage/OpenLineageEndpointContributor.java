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

import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;

/**
 * Advertises the OpenLineage ingest endpoints in the catalog configuration response so clients can
 * discover them during endpoint discovery.
 *
 * <p>Discovered as a CDI {@link CatalogConfigEndpointContributor} bean. Advertisement is gated on
 * {@link FeatureConfiguration#ENABLE_OPENLINEAGE_INGEST}. The routes are always mounted while this
 * extension is assembled into the server, but return {@code 404 Not Found} when the flag is
 * disabled (see {@code OpenLineageAdapter}), so the flag consistently gates both advertisement and
 * request handling.
 */
@ApplicationScoped
@Priority(600)
public class OpenLineageEndpointContributor implements CatalogConfigEndpointContributor {
  private final RealmConfig realmConfig;

  @Inject
  public OpenLineageEndpointContributor(RealmConfig realmConfig) {
    this.realmConfig = realmConfig;
  }

  @Override
  public Set<Endpoint> endpoints() {
    return realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)
        ? OpenLineageEndpoints.OPENLINEAGE_ENDPOINTS
        : ImmutableSet.of();
  }
}
