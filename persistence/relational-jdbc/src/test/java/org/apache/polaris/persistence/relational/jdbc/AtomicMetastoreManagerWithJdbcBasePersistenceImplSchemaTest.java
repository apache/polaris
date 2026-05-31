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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Runs {@link BasePolarisMetaStoreManagerTest} integration tests against every H2 schema version
 * discovered on the classpath.
 */
public class AtomicMetastoreManagerWithJdbcBasePersistenceImplSchemaTest {

  @TestFactory
  Stream<DynamicNode> metastoreTestsForAllH2SchemaVersions() {
    return H2SchemaVersions.discover().stream().map(this::testsForSchemaVersion);
  }

  private DynamicNode testsForSchemaVersion(int schemaVersion) {
    return DynamicContainer.dynamicContainer(
        "schema-v" + schemaVersion,
        testMethods()
            .map(
                method ->
                    DynamicTest.dynamicTest(
                        method.getName(), () -> runTest(method, schemaVersion))));
  }

  private static Stream<Method> testMethods() {
    return Arrays.stream(BasePolarisMetaStoreManagerTest.class.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(Test.class))
        .sorted(Comparator.comparing(Method::getName));
  }

  private static void runTest(Method method, int schemaVersion) throws Exception {
    AtomicMetastoreManagerWithJdbcBasePersistenceImplTest testInstance =
        new SchemaVersionTest(schemaVersion);
    testInstance.setupPolarisMetaStoreManager();
    method.setAccessible(true);
    method.invoke(testInstance);
  }

  private static final class SchemaVersionTest
      extends AtomicMetastoreManagerWithJdbcBasePersistenceImplTest {

    private final int schemaVersion;

    private SchemaVersionTest(int schemaVersion) {
      this.schemaVersion = schemaVersion;
    }

    @Override
    public int schemaVersion() {
      return schemaVersion;
    }
  }
}
