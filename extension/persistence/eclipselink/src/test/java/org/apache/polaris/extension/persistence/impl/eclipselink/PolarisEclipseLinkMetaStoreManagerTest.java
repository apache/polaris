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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneId;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

/**
 * Integration test for EclipseLink based metastore implementation
 *
 * @author aixu
 */
public class PolarisEclipseLinkMetaStoreManagerTest extends BasePolarisMetaStoreManagerTest {

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    PolarisEclipseLinkMetaStoreSessionImpl session =
        new PolarisEclipseLinkMetaStoreSessionImpl(
            store, Mockito.mock(), () -> "realm", null, "polaris");
    return new PolarisTestMetaStoreManager(
        new PolarisMetaStoreManagerImpl(),
        new PolarisCallContext(
            session,
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault())));
  }

  @ParameterizedTest()
  @ArgumentsSource(CreateStoreSessionArgs.class)
  void testCreateStoreSession(String confFile, boolean success) {
    // Clear cache to prevent reuse EntityManagerFactory
    PolarisEclipseLinkMetaStoreSessionImpl.clearEntityManagerFactories();

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    try {
      var session =
          new PolarisEclipseLinkMetaStoreSessionImpl(
              store, Mockito.mock(), () -> "realm", confFile, "polaris");
      assertNotNull(session);
      assertTrue(success);
    } catch (Exception e) {
      assertFalse(success);
    }
  }

  private static class CreateStoreSessionArgs implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
      return Stream.of(
          Arguments.of("META-INF/persistence.xml", true),
          Arguments.of("./build/conf/conf.jar!/persistence.xml", true),
          Arguments.of("/dummy_path/conf.jar!/persistence.xml", false));
    }
  }
}
