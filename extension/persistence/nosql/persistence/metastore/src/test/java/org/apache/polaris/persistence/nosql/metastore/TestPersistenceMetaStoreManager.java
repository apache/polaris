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
package org.apache.polaris.persistence.nosql.metastore;

import static java.time.Clock.systemUTC;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.ids.api.MonotonicClock;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Disabled;

@SuppressWarnings("CdiInjectionPointsInspection")
@EnableWeld
public class TestPersistenceMetaStoreManager extends BasePolarisMetaStoreManagerTest {
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject
  @Identifier("persistence")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject PolarisDiagnostics polarisDiagnostics;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject MonotonicClock monotonicClock;

  String realmId;
  RealmContext realmContext;

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    realmId = UUID.randomUUID().toString();
    realmContext = () -> realmId;

    var startTime = monotonicClock.currentTimeMillis();

    metaStoreManagerFactory.bootstrapRealms(List.of(realmId), RootCredentialsSet.fromEnvironment());

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();

    var callCtx =
        new PolarisCallContext(session, polarisDiagnostics, configurationStore, systemUTC());

    return new PolarisTestMetaStoreManager(manager, callCtx, startTime, false);
  }

  @Override
  @Disabled("No entity cache, no need to test it")
  protected void testEntityCache() {}

  @Override
  @Disabled(
      "Nothing in the code base calls 'loadTasks', the contract of that function is not what the test exercises")
  protected void testLoadTasksInParallel() {}

  @Override
  @Disabled(
      "Nothing in the code base calls 'loadTasks', the contract of that function is not what the test exercises")
  protected void testLoadTasks() {}
}
