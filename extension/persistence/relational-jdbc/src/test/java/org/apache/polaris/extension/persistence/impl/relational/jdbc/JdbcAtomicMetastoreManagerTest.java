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

package org.apache.polaris.extension.persistence.impl.relational.jdbc;

import java.time.ZoneId;

import org.apache.polaris.extension.persistence.relational.jdbc.PolarisJdbcBasePersistenceImpl;
import org.h2.jdbcx.JdbcDataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.extension.persistence.relational.jdbc.DatasourceOperations;
import org.mockito.Mockito;

import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;

import javax.sql.DataSource;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

public class JdbcAtomicMetastoreManagerTest extends BasePolarisMetaStoreManagerTest {

    public static DataSource createH2DataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        // Configure the data source properties
        // In-memory database (data is lost when the JVM exits)
        dataSource.setURL("jdbc:h2:file:./build/test_data/polaris/db;MODE=PostgreSQL;");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        return dataSource;
    }
    @Override
    protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
        PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
        DatasourceOperations datasourceOperations = new DatasourceOperations(createH2DataSource());
        datasourceOperations.executeScript();

        PolarisJdbcBasePersistenceImpl basePersistence =
                new PolarisJdbcBasePersistenceImpl(
                        datasourceOperations, RANDOM_SECRETS, Mockito.mock());
        return new PolarisTestMetaStoreManager(
                new AtomicOperationMetaStoreManager(),
                new PolarisCallContext(
                        basePersistence,
                        diagServices,
                        new PolarisConfigurationStore() {},
                        timeSource.withZone(ZoneId.systemDefault())));
    }
}
