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
package org.apache.polaris.service.quarkus.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Clock;

import jakarta.ws.rs.container.ContainerRequestContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.context.RealmContextResolver;
import org.junit.jupiter.api.TestInfo;

@Singleton
public class PolarisIntegrationTestHelper {

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject RealmContextResolver realmContextResolver;
  @Inject ObjectMapper objectMapper;
  @Inject PolarisDiagnostics diagServices;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject Clock clock;

  public PolarisIntegrationTestFixture createFixture(TestEnvironment testEnv, TestInfo testInfo) {
    return new PolarisIntegrationTestFixture(this, testEnv, testInfo);
  }
}
