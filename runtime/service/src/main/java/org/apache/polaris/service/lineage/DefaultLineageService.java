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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.apache.polaris.core.lineage.LineageService;

@RequestScoped
public class DefaultLineageService implements LineageService {
  private final CallContext callContext;
  private final LineageConfiguration configuration;

  @Inject
  public DefaultLineageService(CallContext callContext, LineageConfiguration configuration) {
    this.callContext = callContext;
    this.configuration = configuration;
  }

  @Override
  public LineageGraph query(LineageQueryRequest request) {
    ensureEnabled();
    throw new UnsupportedOperationException("Lineage query is not implemented yet.");
  }

  private void ensureEnabled() {
    if (!configuration.enabled()) {
      throw new UnsupportedOperationException(
          "Lineage is disabled: set polaris.lineage.enabled=true to enable it.");
    }

    if (!callContext.getRealmConfig().getConfig(FeatureConfiguration.ENABLE_LINEAGE)) {
      throw new UnsupportedOperationException(
          "Lineage realm feature is disabled: enable "
              + FeatureConfiguration.ENABLE_LINEAGE.key()
              + " in the realm feature configuration.");
    }
  }
}
