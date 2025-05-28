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
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.identity.mutation.EntityMutationEngine;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public class QuarkusProducers {

  @Produces
  public MetaStoreManagerFactory metaStoreManagerFactory(
      @ConfigProperty(name = "polaris.persistence.type") String persistenceType,
      @Any Instance<MetaStoreManagerFactory> metaStoreManagerFactories) {
    return metaStoreManagerFactories.select(Identifier.Literal.of(persistenceType)).get();
  }

  // CDI dependencies of EclipseLink's MetaStoreManagerFactory:

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
  public EntityMutationEngine entityMutationEngine() {
    // An entity mutation engine is not required when running the admin tool.
    return entity -> entity;
  }

  @Produces
  public PolarisConfigurationStore configurationStore() {
    // A configuration store is not required when running the admin tool.
    return new PolarisConfigurationStore() {};
  }
}
