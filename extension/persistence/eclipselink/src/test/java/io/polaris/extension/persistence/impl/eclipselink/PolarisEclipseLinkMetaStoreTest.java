/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.extension.persistence.impl.eclipselink;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import io.polaris.core.persistence.PolarisMetaStoreManagerTest;
import io.polaris.core.persistence.PolarisTestMetaStoreManager;
import java.time.ZoneId;
import java.util.stream.Stream;
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
public class PolarisEclipseLinkMetaStoreTest extends PolarisMetaStoreManagerTest {

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    PolarisEclipseLinkMetaStoreSessionImpl session =
        new PolarisEclipseLinkMetaStoreSessionImpl(
            store, Mockito.mock(), () -> "realm", null, "polaris-dev");
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
    // Clear to test out EntityManagerFactory creation rather than reusing
    PolarisEclipseLinkMetaStoreSessionImpl.clearEntityManagerFactory();

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    try {
      var session =
          new PolarisEclipseLinkMetaStoreSessionImpl(
              store, Mockito.mock(), () -> "realm", confFile, "polaris-dev");
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
          Arguments.of("eclipselink_conf.jar!/persistence.xml", true),
          Arguments.of("/dummy_path/conf.jar!/persistence.xml", false));
    }
  }
}
