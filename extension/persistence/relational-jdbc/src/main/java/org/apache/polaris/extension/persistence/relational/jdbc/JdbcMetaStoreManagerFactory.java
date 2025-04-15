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
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.*;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using a JDBC backed by SQL metastore.
 */
@ApplicationScoped
@Identifier("relational-jdbc")
public class JdbcMetaStoreManagerFactory extends LocalPolarisMetaStoreManagerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetaStoreManagerFactory.class);

  // TODO: Pending discussion of if we should have one Database per realm or 1 schema per realm
  // or realm should be a primary key on all the tables.
  @Inject DataSource dataSource;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;

  protected JdbcMetaStoreManagerFactory() {
    this(null);
  }

  protected JdbcMetaStoreManagerFactory(@Nonnull PolarisDiagnostics diagnostics) {
    super(diagnostics);
  }

  /**
   * Subclasses can override this to inject different implementations of PolarisMetaStoreManager
   * into the existing realm-based setup flow.
   */
  @Override
  protected PolarisMetaStoreManager createNewMetaStoreManager() {
    return new AtomicOperationMetaStoreManager();
  }

  @Override
  protected void initializeForRealm(
      RealmContext realmContext, RootCredentialsSet rootCredentialsSet) {
    DatasourceOperations databaseOperations = new DatasourceOperations(dataSource);
    // TODO: see if we need to take script from Quarkus or can we just
    // use the script committed in the repo.
    try {
      databaseOperations.executeScript("scripts/postgres/schema-v1-postgres.sql");
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Error executing sql script: %s", e.getMessage()), e);
    }
    sessionSupplierMap.put(
        realmContext.getRealmIdentifier(),
        () ->
            new JdbcBasePersistenceImpl(
                databaseOperations,
                secretsGenerator(realmContext, rootCredentialsSet),
                storageIntegrationProvider,
                realmContext.getRealmIdentifier()));

    PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
    metaStoreManagerMap.put(realmContext.getRealmIdentifier(), metaStoreManager);
  }
}
