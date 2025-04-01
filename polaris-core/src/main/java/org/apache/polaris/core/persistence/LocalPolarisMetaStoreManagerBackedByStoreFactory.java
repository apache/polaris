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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;

public abstract class LocalPolarisMetaStoreManagerBackedByStoreFactory<StoreType>
    extends LocalPolarisMetaStoreManagerFactory {

  final Map<String, StoreType> backingStoreMap = new HashMap<>();

  protected LocalPolarisMetaStoreManagerBackedByStoreFactory(
      @Nonnull PolarisDiagnostics diagnostics) {
    super(diagnostics);
  }

  protected abstract StoreType createBackingStore(@Nonnull PolarisDiagnostics diagnostics);

  protected abstract TransactionalPersistence createMetaStoreSession(
      @Nonnull StoreType store,
      @Nonnull RealmContext realmContext,
      @Nullable RootCredentialsSet rootCredentialsSet,
      @Nonnull PolarisDiagnostics diagnostics);

  @Override
  protected void initializeForRealm(
      RealmContext realmContext, RootCredentialsSet rootCredentialsSet) {
    final StoreType backingStore = createBackingStore(diagnostics);
    sessionSupplierMap.put(
        realmContext.getRealmIdentifier(),
        () -> createMetaStoreSession(backingStore, realmContext, rootCredentialsSet, diagnostics));

    PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
    metaStoreManagerMap.put(realmContext.getRealmIdentifier(), metaStoreManager);
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> resultMap = super.purgeRealms(realms);
    for (String realm : realms) {
      backingStoreMap.remove(realm);
    }
    return resultMap;
  }
}
