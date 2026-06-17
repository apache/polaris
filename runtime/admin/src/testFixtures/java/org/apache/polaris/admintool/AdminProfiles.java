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
package org.apache.polaris.admintool;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.List;
import java.util.Map;
import org.apache.polaris.test.commons.CockroachRelationalJdbcLifeCycleManagement;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;

public final class AdminProfiles {

  private AdminProfiles() {}

  public static class NoSqlInMemory implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.persistence.type", "nosql", "polaris.persistence.nosql.backend", "InMemory");
    }
  }

  public static class NoSqlMongo implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.persistence.type", "nosql", "polaris.persistence.nosql.backend", "MongoDb");
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(MongoTestResourceLifecycleManager.class));
    }
  }

  public static class RelationalJdbc implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(
          new TestResourceEntry(PostgresRelationalJdbcLifeCycleManagement.class, Map.of()));
    }
  }

  public static class CockroachJdbc implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(
          new TestResourceEntry(CockroachRelationalJdbcLifeCycleManagement.class, Map.of()));
    }
  }
}
