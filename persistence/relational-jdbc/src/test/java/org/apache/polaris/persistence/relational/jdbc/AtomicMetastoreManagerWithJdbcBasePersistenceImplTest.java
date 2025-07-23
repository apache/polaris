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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.io.InputStream;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.h2.jdbcx.JdbcConnectionPool;
import org.mockito.Mockito;

public class AtomicMetastoreManagerWithJdbcBasePersistenceImplTest
    extends BasePolarisMetaStoreManagerTest {

  public static DataSource createH2DataSource() {
    return JdbcConnectionPool.create("jdbc:h2:file:./build/test_data/polaris/db", "sa", "");
  }

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    DatasourceOperations datasourceOperations;
    try {
      datasourceOperations =
          new DatasourceOperations(createH2DataSource(), new H2JdbcConfiguration());
      ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
      InputStream scriptStream =
          classLoader.getResourceAsStream(
              String.format("%s/schema-v2.sql", DatabaseType.H2.getDisplayName()));
      datasourceOperations.executeScript(scriptStream);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Error executing %s script: %s", DatabaseType.H2.getDisplayName(), e.getMessage()),
          e);
    }

    RealmContext realmContext = () -> "REALM";
    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            datasourceOperations,
            RANDOM_SECRETS,
            Mockito.mock(),
            realmContext.getRealmIdentifier());
    return new PolarisTestMetaStoreManager(
        new AtomicOperationMetaStoreManager(),
        new PolarisCallContext(
            realmContext,
            basePersistence,
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault())));
  }

  private static class H2JdbcConfiguration implements RelationalJdbcConfiguration {

    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(2);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }
  }
}
