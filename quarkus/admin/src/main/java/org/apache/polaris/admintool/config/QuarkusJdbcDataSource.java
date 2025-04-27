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

import io.quarkus.arc.InstanceHandle;
import org.apache.polaris.extension.persistence.relational.jdbc.JdbcDatasource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class QuarkusJdbcDataSource implements JdbcDatasource {
    private List<InstanceHandle<DataSource>> dataSources;

    public static final String DEFAULT_DATA_SOURCE_NAME = "<default>";

    public QuarkusJdbcDataSource(List<InstanceHandle<DataSource>> dataSources) {
        this.dataSources = dataSources;
    }

    @Override
    public DataSource fromRealmId(String realmId) {
        for (InstanceHandle<DataSource> handle : dataSources) {
            String name = handle.getBean().getName();
            name = name == null ? DEFAULT_DATA_SOURCE_NAME : unquoteDataSourceName(name);
            if (name.equals(realmId)) {
                return handle.get();
            }
        }
        throw new IllegalStateException("No datasource configured with name: " + realmId);
    }

    public static String unquoteDataSourceName(String dataSourceName) {
        if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
            dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
        }
        return dataSourceName;
    }
}
