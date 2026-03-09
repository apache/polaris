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

import java.util.Set;
import javax.sql.DataSource;

/**
 * Service to resolve the correct {@link DataSource} for a given realm and store
 * type.
 * This enables isolating different workloads (e.g., entity metadata vs metrics
 * vs events)
 * into different physical databases or connection pools.
 */
public interface DataSourceResolver {

    /** The type of store representing the workload pattern. */
    enum StoreType {
        MAIN,
        METRICS,
        EVENTS
    }

    /**
     * Resolves the DataSource for a given realm and store type.
     *
     * @param realmId   the realm identifier
     * @param storeType the type of store (e.g., main, metrics, events)
     * @return the resolved DataSource
     */
    DataSource resolve(String realmId, StoreType storeType);
}
