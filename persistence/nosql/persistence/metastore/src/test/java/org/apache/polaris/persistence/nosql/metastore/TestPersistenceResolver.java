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

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.BaseResolverTest;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.ids.api.MonotonicClock;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.BeforeAll;

@SuppressWarnings("CdiInjectionPointsInspection")
@EnableWeld
public class TestPersistenceResolver extends BaseResolverTest {
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject
  @Identifier("nosql")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject PolarisConfigurationStore configurationStore;
  @Inject MonotonicClock monotonicClock;

  PolarisMetaStoreManager metaStoreManager;

  PolarisCallContext callCtx;
  PolarisTestMetaStoreManager tm;

  String realmId;
  RealmContext realmContext;

  @Override
  protected PolarisCallContext callCtx() {
    if (callCtx == null) {
      realmId = UUID.randomUUID().toString();
      realmContext = () -> realmId;

      var startTime = monotonicClock.currentTimeMillis();

      metaStoreManagerFactory.bootstrapRealms(
          List.of(realmId), RootCredentialsSet.fromEnvironment());

      metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
      var session = metaStoreManagerFactory.getOrCreateSession(realmContext);

      callCtx = new PolarisCallContext(realmContext, session, configurationStore);

      tm = new PolarisTestMetaStoreManager(metaStoreManager, callCtx, startTime, false);
    }
    return callCtx;
  }

  @Override
  protected PolarisTestMetaStoreManager tm() {
    callCtx();
    return tm;
  }

  @Override
  protected PolarisMetaStoreManager metaStoreManager() {
    callCtx();
    return metaStoreManager;
  }

  @Override
  protected void checkRefGrantRecords(
      List<PolarisGrantRecord> grantRecords, List<PolarisGrantRecord> refGrantRecords) {
    assertThat(grantRecords).containsExactlyInAnyOrderElementsOf(refGrantRecords);
  }

  @BeforeAll
  public static void beforeAll() {
    supportEntityCache = false;
  }
}
