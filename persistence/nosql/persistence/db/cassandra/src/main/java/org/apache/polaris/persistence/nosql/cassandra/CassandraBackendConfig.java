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
package org.apache.polaris.persistence.nosql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import java.time.Duration;
import java.util.Set;

public record CassandraBackendConfig(
    CqlSession client,
    String keyspace,
    Duration ddlTimeout,
    Duration dmlTimeout,
    DefaultConsistencyLevel consistencyLevel,
    DefaultConsistencyLevel serialConsistencyLevel,
    boolean closeClient) {
  public CassandraBackendConfig {
    validateConsistencyLevels(consistencyLevel, serialConsistencyLevel);
  }

  static void validateConsistencyLevels(
      DefaultConsistencyLevel consistencyLevel, DefaultConsistencyLevel serialConsistencyLevel) {
    if (!Set.of(
            DefaultConsistencyLevel.LOCAL_QUORUM,
            DefaultConsistencyLevel.QUORUM,
            DefaultConsistencyLevel.ALL)
        .contains(consistencyLevel)) {
      throw new IllegalArgumentException(
          "Cassandra consistency-level must be LOCAL_QUORUM, QUORUM, or ALL.");
    }
    if (!serialConsistencyLevel.isSerial()) {
      throw new IllegalArgumentException(
          "Cassandra serial-consistency-level must be LOCAL_SERIAL or SERIAL.");
    }
  }

  public static final String DEFAULT_KEYSPACE = "polaris";
  public static final String DEFAULT_DDL_TIMEOUT = "PT5S";
  public static final String DEFAULT_DML_TIMEOUT = "PT3S";
}
