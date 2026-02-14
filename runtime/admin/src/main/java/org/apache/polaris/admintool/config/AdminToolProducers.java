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
package org.apache.polaris.admintool.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;
import java.util.UUID;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

public class AdminToolProducers {

  @Produces
  public MetaStoreManagerFactory metaStoreManagerFactory(
      QuarkusPersistenceConfiguration persistenceConfiguration,
      @Any Instance<MetaStoreManagerFactory> metaStoreManagerFactories) {
    return metaStoreManagerFactories
        .select(Identifier.Literal.of(persistenceConfiguration.type()))
        .get();
  }

  @Produces
  @ApplicationScoped
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Produces
  @ApplicationScoped
  public PolarisDiagnostics polarisDiagnostics() {
    return new PolarisDefaultDiagServiceImpl();
  }

  @Produces
  public PolarisStorageIntegrationProvider storageIntegrationProvider() {
    // A storage integration provider is not required when running the admin tool.
    return new PolarisStorageIntegrationProvider() {
      @Override
      @Nullable
      public <T extends PolarisStorageConfigurationInfo>
          PolarisStorageIntegration<T> getStorageIntegrationForConfig(
              PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
        return null;
      }
    };
  }

  @Produces
  public RealmConfigurationSource configurationStore() {
    // A configuration source is not required when running the admin tool.
    return RealmConfigurationSource.EMPTY_CONFIG;
  }

  @Produces
  public RealmContext dummyRealmContext() {
    // Use UUID to protect against accidental realm ID collisions.
    // This is a dummy RealmContext for the admin tool - required by JdbcMetricsPersistenceProducer
    // but not actually used since the admin tool doesn't persist metrics.
    String absentId = UUID.randomUUID().toString();
    return () -> absentId;
  }

  @Produces
  public RealmConfig dummyRealmConfig(
      RealmConfigurationSource configurationSource, RealmContext realmContext) {
    return new RealmConfigImpl(configurationSource, realmContext);
  }
}
