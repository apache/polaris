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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.polaris.extension.persistence.relational.jdbc.ConnectionManager;
import org.junit.jupiter.api.Test;

public class ConnectionManagerTest {

  private ConnectionManager connectionManager;

  @Test
  void testReadProperties() throws Exception {
    // To test the db.properties is correctly read and properties are set correctly.
    BasicDataSource bds = (BasicDataSource) new ConnectionManager("db.properties").getDataSource();
    assertEquals("jdbc:postgresql://localhost:5432/postgres", bds.getUrl());
    assertEquals("sa", bds.getUsername());
    assertEquals("", bds.getPassword());
    assertEquals("org.postgresql.Driver", bds.getDriverClassName());
    assertEquals(5, bds.getInitialSize());
    assertEquals(20, bds.getMaxTotal());
  }

  @Test
  void testCloseDataSource() throws Exception {
    // Mocking BasicDataSource to verify that close is called
    BasicDataSource mockBds = mock(BasicDataSource.class);
    connectionManager = new ConnectionManager("db.properties");
    connectionManager.setDataSource(mockBds);

    connectionManager.closeDataSource();
    verify(mockBds).close(); // Ensure that close() was called on BasicDataSource
  }

  @Test
  void testCloseDataSourceWithException() throws Exception {
    // Test case where closing data source throws an exception
    BasicDataSource mockBds = mock(BasicDataSource.class);
    doThrow(new Exception("Error closing")).when(mockBds).close();

    connectionManager = new ConnectionManager("db.properties");
    connectionManager.setDataSource(mockBds);

    Exception exception = assertThrows(Exception.class, () -> connectionManager.closeDataSource());
    assertEquals("Error closing", exception.getMessage());
  }
}
