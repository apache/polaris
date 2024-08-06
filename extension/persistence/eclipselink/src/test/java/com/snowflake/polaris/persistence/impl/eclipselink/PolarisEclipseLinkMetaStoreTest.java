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
package com.snowflake.polaris.persistence.impl.eclipselink;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import io.polaris.core.persistence.PolarisMetaStoreManagerTest;
import io.polaris.core.persistence.PolarisTestMetaStoreManager;
import io.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkMetaStoreSessionImpl;
import io.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Integration test for EclipseLink based metastore implementation
 *
 * @author aixu
 */
public class PolarisEclipseLinkMetaStoreTest extends PolarisMetaStoreManagerTest {

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    Path tmpFile = Paths.get(String.format("/tmp/%s-persistence.xml", UUID.randomUUID()));
    try {
      Files.copy(
          Objects.requireNonNull(getClass().getResourceAsStream("/META-INF/persistence.xml")),
          tmpFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    PolarisEclipseLinkMetaStoreSessionImpl session =
        new PolarisEclipseLinkMetaStoreSessionImpl(
            store, Mockito.mock(), () -> "realm", tmpFile.toString(), "polaris-dev");
    return new PolarisTestMetaStoreManager(
        new PolarisMetaStoreManagerImpl(),
        new PolarisCallContext(
            session,
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault())));
  }

  @Test
  void throwExceptionIfConfigFileDoesNotExists() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    new PolarisEclipseLinkMetaStoreSessionImpl(
        store, Mockito.mock(), () -> "realm", "does not exists", "polaris-dev");
  }

  @Test
  void ensureNotLoadingDefaultClassPersistence() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    new PolarisEclipseLinkMetaStoreSessionImpl(
        store, Mockito.mock(), () -> "realm", null, "polaris-dev");
  }
}
