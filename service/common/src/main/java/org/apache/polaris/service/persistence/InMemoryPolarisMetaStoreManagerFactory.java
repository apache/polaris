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
package org.apache.polaris.service.persistence;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

@ApplicationScoped
@Identifier("in-memory")
public class InMemoryPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<TreeMapMetaStore> {

  private final PolarisStorageIntegrationProvider storageIntegration;
  private final Set<String> bootstrappedRealms = new HashSet<>();

  public InMemoryPolarisMetaStoreManagerFactory() {
    this(null, null);
  }

  @Inject
  public InMemoryPolarisMetaStoreManagerFactory(
      PolarisStorageIntegrationProvider storageIntegration, PolarisDiagnostics diagnostics) {
    super(diagnostics);
    this.storageIntegration = storageIntegration;
  }

  @Override
  protected TreeMapMetaStore createBackingStore(@Nonnull PolarisDiagnostics diagnostics) {
    return new TreeMapMetaStore(diagnostics);
  }

  @Override
  protected TransactionalPersistence createMetaStoreSession(
      @Nonnull TreeMapMetaStore store,
      @Nonnull RealmContext realmContext,
      @Nullable RootCredentialsSet rootCredentialsSet,
      @Nonnull PolarisDiagnostics diagnostics) {
    return new TreeMapTransactionalPersistenceImpl(
        store, storageIntegration, secretsGenerator(realmContext, rootCredentialsSet));
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmsFromEnvironment(List.of(realmId));
    }
    return super.getOrCreateMetaStoreManager(realmContext);
  }

  @Override
  public synchronized Supplier<TransactionalPersistence> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmsFromEnvironment(List.of(realmId));
    }
    return super.getOrCreateSessionSupplier(realmContext);
  }

  private void bootstrapRealmsFromEnvironment(List<String> realms) {
    RootCredentialsSet rootCredentialsSet = RootCredentialsSet.fromEnvironment();
    this.bootstrapRealms(realms, rootCredentialsSet);
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    Map<String, PrincipalSecretsResult> results = super.bootstrapRealms(realms, rootCredentialsSet);
    bootstrappedRealms.addAll(results.keySet());
    return results;
  }
}
