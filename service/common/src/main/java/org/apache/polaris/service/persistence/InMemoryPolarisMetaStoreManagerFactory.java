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
<<<<<<< HEAD
import java.time.Clock;
import java.util.List;
=======
import java.util.Collections;
import java.util.HashSet;
>>>>>>> parent of b84f4624 (Remove CallContext and its ThreadLocal usage (#589))
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisCredentialsBootstrap;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.PolarisTreeMapStore;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.service.context.RealmContextConfiguration;

@ApplicationScoped
@Identifier("in-memory")
public class InMemoryPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<PolarisTreeMapStore> {

  private final PolarisStorageIntegrationProvider storageIntegration;
  private final Set<String> bootstrappedRealms = new HashSet<>();

  public InMemoryPolarisMetaStoreManagerFactory() {
    this(null);
  }

  @Inject
  public InMemoryPolarisMetaStoreManagerFactory(
      PolarisStorageIntegrationProvider storageIntegration) {
    this.storageIntegration = storageIntegration;
  }

  public void onStartup(RealmContextConfiguration realmContextConfiguration) {
    bootstrapRealmsAndPrintCredentials(realmContextConfiguration.realms());
  }

  @Override
  protected PolarisTreeMapStore createBackingStore(@Nonnull PolarisDiagnostics diagnostics) {
    return new PolarisTreeMapStore(diagnostics);
  }

  @Override
  protected PolarisMetaStoreSession createMetaStoreSession(
<<<<<<< HEAD
      @Nonnull PolarisTreeMapStore store,
      @Nonnull RealmContext realmContext,
      @Nullable PolarisCredentialsBootstrap credentialsBootstrap,
      @Nonnull PolarisDiagnostics diagnostics) {
    return new PolarisTreeMapMetaStoreSessionImpl(
        store,
        storageIntegration,
        secretsGenerator(realmContext, credentialsBootstrap),
        diagnostics);
=======
      @Nonnull PolarisTreeMapStore store, @Nonnull RealmContext realmContext) {
    return new PolarisTreeMapMetaStoreSessionImpl(
        store, storageIntegration, secretsGenerator(realmContext));
>>>>>>> parent of b84f4624 (Remove CallContext and its ThreadLocal usage (#589))
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmsAndPrintCredentials(List.of(realmId));
    }
    return super.getOrCreateMetaStoreManager(realmContext);
  }

  @Override
  public synchronized Supplier<PolarisMetaStoreSession> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmsAndPrintCredentials(List.of(realmId));
    }
    return super.getOrCreateSessionSupplier(realmContext);
  }

  private void bootstrapRealmsAndPrintCredentials(List<String> realms) {
    PolarisCredentialsBootstrap credentialsBootstrap =
        PolarisCredentialsBootstrap.fromEnvironment();
    Map<String, PrincipalSecretsResult> results =
        this.bootstrapRealms(realms, credentialsBootstrap);
    bootstrappedRealms.addAll(realms);

    for (String realmId : realms) {
      PrincipalSecretsResult principalSecrets = results.get(realmId);

      String msg =
          String.format(
              "realm: %1s root principal credentials: %2s:%3s",
              realmId,
              principalSecrets.getPrincipalSecrets().getPrincipalClientId(),
              principalSecrets.getPrincipalSecrets().getMainSecret());
      System.out.println(msg);
    }
  }
}
