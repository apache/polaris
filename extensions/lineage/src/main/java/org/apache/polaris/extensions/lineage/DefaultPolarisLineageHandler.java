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
package org.apache.polaris.extensions.lineage;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;

@RequestScoped
public class DefaultPolarisLineageHandler implements PolarisLineageHandler {
  private final CallContext callContext;
  private final LineageConfiguration configuration;
  private final LineageStoreManager storeManager;

  @Inject
  public DefaultPolarisLineageHandler(
      CallContext callContext,
      LineageConfiguration configuration,
      LineageStoreManager storeManager) {
    this.callContext = callContext;
    this.configuration = configuration;
    this.storeManager = storeManager;
  }

  @Override
  public void ingest(LineageIngestRequest request) {
    ensureEnabled();
    Instant lastEventAt = request.eventTime().orElseGet(Instant::now);
    storeManager.upsertDatasets(request.datasets());
    storeManager.replaceDatasetEdges(request.edges(), lastEventAt);
    storeManager.upsertColumnEdges(request.columnEdges(), lastEventAt);
  }

  @Override
  public LineageGraph query(LineageQueryRequest request) {
    ensureEnabled();
    return storeManager.loadLineage(request);
  }

  private void ensureEnabled() {
    if (!configuration.enabled()) {
      throw new UnsupportedOperationException(
          "Lineage is disabled: set polaris.lineage.enabled=true to enable it.");
    }

    if (!configuration.persistence().enabled()) {
      throw new UnsupportedOperationException(
          "Lineage store manager is disabled: set polaris.lineage.persistence.enabled=true to enable"
              + " it.");
    }

    if (!callContext.getRealmConfig().getConfig(FeatureConfiguration.ENABLE_LINEAGE)) {
      throw new UnsupportedOperationException(
          "Lineage realm feature is disabled: enable "
              + FeatureConfiguration.ENABLE_LINEAGE.key()
              + " in the realm feature configuration.");
    }
  }
}
