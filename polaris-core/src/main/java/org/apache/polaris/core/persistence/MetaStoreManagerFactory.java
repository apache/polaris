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

import java.util.Map;
import java.util.function.Supplier;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;

/** Configuration interface for configuring the {@link PolarisMetaStoreManager}. */
public interface MetaStoreManagerFactory {

  PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext);

  Supplier<? extends BasePersistence> getOrCreateSessionSupplier(RealmContext realmContext);

  EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig);

  Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet);

  default Map<String, PrincipalSecretsResult> bootstrapRealms(BootstrapOptions bootstrapOptions) {
    return bootstrapRealms(bootstrapOptions.realms(), bootstrapOptions.rootCredentialsSet());
  }

  /** Purge all metadata for the realms provided */
  Map<String, BaseResult> purgeRealms(Iterable<String> realms);
}
