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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.file.Path;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using an EclipseLink based meta store to store and retrieve all Polaris metadata. It can be
 * configured through persistence.xml to use supported RDBMS as the meta store.
 */
@ApplicationScoped
@Identifier("eclipse-link")
public class EclipseLinkPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<PolarisEclipseLinkStore> {

  @Inject EclipseLinkConfiguration eclipseLinkConfiguration;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;

  protected EclipseLinkPolarisMetaStoreManagerFactory() {
    this(null);
  }

  @Inject
  protected EclipseLinkPolarisMetaStoreManagerFactory(PolarisDiagnostics diagnostics) {
    super(diagnostics);
  }

  @Override
  protected PolarisEclipseLinkStore createBackingStore(@Nonnull PolarisDiagnostics diagnostics) {
    return new PolarisEclipseLinkStore(diagnostics);
  }

  @Override
  protected TransactionalPersistence createMetaStoreSession(
      @Nonnull PolarisEclipseLinkStore store,
      @Nonnull RealmContext realmContext,
      @Nullable RootCredentialsSet rootCredentialsSet,
      @Nonnull PolarisDiagnostics diagnostics) {
    return new PolarisEclipseLinkMetaStoreSessionImpl(
        store,
        storageIntegrationProvider,
        realmContext,
        configurationFile(),
        persistenceUnitName(),
        secretsGenerator(realmContext, rootCredentialsSet));
  }

  private String configurationFile() {
    return eclipseLinkConfiguration.configurationFile().map(Path::toString).orElse(null);
  }

  private String persistenceUnitName() {
    return eclipseLinkConfiguration.configurationFile().isPresent()
        ? eclipseLinkConfiguration.persistenceUnit()
        : null;
  }
}
