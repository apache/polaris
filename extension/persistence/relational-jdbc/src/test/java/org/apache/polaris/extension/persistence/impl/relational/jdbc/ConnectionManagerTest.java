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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.polaris.extension.persistence.relational.jdbc.ConnectionManager;
import org.apache.polaris.extension.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ConnectionManagerTest.Profile.class)
public class ConnectionManagerTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.persistence.relational.jdbc-url",
          "jdbc:postgresql://localhost:5432/postgres",
          "polaris.persistence.relational.user-name",
          "sa",
          "polaris.persistence.relational.password",
          "pp",
          "polaris.persistence.relational.driver-class-name",
          "org.postgresql.Driver");
    }
  }

  @BeforeEach
  public void beforeEach() {
    this.connectionManager = new ConnectionManager(relationalJdbcConfiguration);
  }

  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;
  ConnectionManager connectionManager;

  @Test
  void testReadProperties() {
    // To test the db.properties is correctly read and properties are set correctly.
    BasicDataSource bds = (BasicDataSource) connectionManager.getDataSource();
    assertEquals("jdbc:postgresql://localhost:5432/postgres", bds.getUrl());
    assertEquals("sa", bds.getUsername());
    assertEquals("pp", bds.getPassword());
    assertEquals("org.postgresql.Driver", bds.getDriverClassName());
  }

  @Test
  void testCloseDataSource() throws Exception {
    // Mocking BasicDataSource to verify that close is called
    BasicDataSource mockBds = mock(BasicDataSource.class);
    connectionManager.setDataSource(mockBds);

    connectionManager.closeDataSource();
    verify(mockBds).close(); // Ensure that close() was called on BasicDataSource
  }

  @Test
  void testCloseDataSourceWithException() throws Exception {
    // Test case where closing data source throws an exception
    BasicDataSource mockBds = mock(BasicDataSource.class);
    doThrow(new Exception("Error closing")).when(mockBds).close();

    connectionManager.setDataSource(mockBds);

    Exception exception = assertThrows(Exception.class, () -> connectionManager.closeDataSource());
    assertEquals("Error closing", exception.getMessage());
  }
}
