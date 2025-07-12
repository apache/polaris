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
package org.apache.polaris.persistence.nosql.coretypes.maintenance;

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

@ApplicationScoped
public class CdiProducers {
  public static MutableCatalogsMaintenanceConfig config = new MutableCatalogsMaintenanceConfig();

  @Produces
  MutableCatalogsMaintenanceConfig produceMutableCatalogsMaintenanceConfig() {
    return config;
  }

  @Produces
  PolarisStorageIntegrationProvider producePolarisStorageIntegrationProvider() {
    return new PolarisStorageIntegrationProvider() {
      @Override
      public @Nullable <T extends PolarisStorageConfigurationInfo>
          PolarisStorageIntegration<T> getStorageIntegrationForConfig(
              PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Produces
  PolarisConfigurationStore producePolarisConfigurationStore() {
    return new PolarisConfigurationStore() {};
  }

  @Produces
  PolarisDiagnostics producePolarisDiagnostics() {
    return new PolarisDefaultDiagServiceImpl();
  }

  @Produces
  Clock produceClock() {
    return Clock.systemUTC();
  }
}
