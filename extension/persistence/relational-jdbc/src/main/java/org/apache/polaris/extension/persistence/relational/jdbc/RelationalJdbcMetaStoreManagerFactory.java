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
package org.apache.polaris.extension.persistence.relational.jdbc;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;

@ApplicationScoped
@Identifier("relational-jdbc")
public class RelationalJdbcMetaStoreManagerFactory implements MetaStoreManagerFactory {

  @Inject RelationalJdbcConfiguration jdbcConfiguration;

  // TODO: Implement MetaStoreManagerFactory methods.
  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  @Override
  public Supplier<? extends BasePersistence> getOrCreateSessionSupplier(RealmContext realmContext) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  @Override
  public StorageCredentialCache getOrCreateStorageCredentialCache(RealmContext realmContext) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    throw new UnsupportedOperationException("Relational jdbc metastore manager is not supported");
  }

  private String configurationFile() {
    return jdbcConfiguration.configurationFile().map(Path::toString).orElse(null);
  }
}
