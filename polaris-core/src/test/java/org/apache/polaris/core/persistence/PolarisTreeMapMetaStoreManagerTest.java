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
package org.apache.polaris.core.persistence;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.time.ZoneId;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.dao.PolarisMetastoreManagerDao;
import org.apache.polaris.core.persistence.transactional.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.transactional.PolarisTreeMapStore;
import org.mockito.Mockito;

public class PolarisTreeMapMetaStoreManagerTest extends BasePolarisMetaStoreManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisTreeMapStore store = new PolarisTreeMapStore(diagServices);
    PolarisCallContext callCtx =
        new PolarisCallContext(
            new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock(), RANDOM_SECRETS),
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault()));

    // TODO: PolarisTreeMapMetaStoreSessionImpl now resides within a persistence implementation
    // layer, below the DAO layer,
    // and ideally shouldn't directly invoke DAO classes. The change is temporarily for refactor
    // verification purposes.
    // We should identify a cleaner testing strategy moving forward.
    return new PolarisTestMetaStoreManager(new PolarisMetastoreManagerDao(), callCtx);
  }
}
