package io.polaris.core.persistence;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import java.time.ZoneId;
import org.mockito.Mockito;

public class PolarisTreeMapMetaStoreManagerTest extends PolarisMetaStoreManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisTreeMapStore store = new PolarisTreeMapStore(diagServices);
    PolarisCallContext callCtx =
        new PolarisCallContext(
            new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock()),
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault()));

    return new PolarisTestMetaStoreManager(new PolarisMetaStoreManagerImpl(), callCtx);
  }
}
