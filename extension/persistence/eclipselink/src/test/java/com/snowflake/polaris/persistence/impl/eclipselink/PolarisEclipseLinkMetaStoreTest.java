/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
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
import java.time.ZoneId;
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
}
