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
package org.apache.polaris.service.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.service.config.AuthorizationConfiguration;
import org.apache.polaris.service.config.ServiceProducers;
import org.junit.jupiter.api.Test;

public class ServiceProducersIT {

  public static class InternalAuthorizationConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "internal");
      return config;
    }
  }

  @QuarkusTest
  @TestProfile(ServiceProducersIT.InternalAuthorizationConfig.class)
  public static class InternalAuthorizationTest {

    @Inject PolarisAuthorizer polarisAuthorizer;

    @Test
    void testInternalPolarisAuthorizerProduced() {
      assertThat(polarisAuthorizer).isNotNull();
    }
  }

  public static class PerRealmAuthorizationConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "internal");
      config.put("polaris.authorization.realm1.type", "test-authorizer");
      return config;
    }
  }

  @QuarkusTest
  @TestProfile(ServiceProducersIT.PerRealmAuthorizationConfig.class)
  public static class PerRealmAuthorizationTest {

    @Inject ServiceProducers serviceProducers;
    @Inject AuthorizationConfiguration authorizationConfiguration;
    @Inject @Any Instance<PolarisAuthorizerFactory> authorizerFactories;

    @Test
    void testPerRealmAuthorizerSelection() {
      PolarisAuthorizer realmAuthorizer =
          serviceProducers.polarisAuthorizer(
              () -> "realm1",
              authorizationConfiguration,
              authorizerFactories,
              new RealmConfigImpl(RealmConfigurationSource.EMPTY_CONFIG, () -> "realm1"));
      assertThat(realmAuthorizer).isInstanceOf(TestPolarisAuthorizer.class);

      PolarisAuthorizer defaultAuthorizer =
          serviceProducers.polarisAuthorizer(
              () -> "other",
              authorizationConfiguration,
              authorizerFactories,
              new RealmConfigImpl(RealmConfigurationSource.EMPTY_CONFIG, () -> "other"));
      assertThat(defaultAuthorizer).isInstanceOf(PolarisAuthorizerImpl.class);
    }
  }

  @ApplicationScoped
  @Identifier("test-authorizer")
  public static class TestPolarisAuthorizerFactory implements PolarisAuthorizerFactory {
    @Override
    public PolarisAuthorizer create(RealmConfig realmConfig) {
      return new TestPolarisAuthorizer();
    }
  }

  public static class TestPolarisAuthorizer implements PolarisAuthorizer {
    @Override
    public void resolveAuthorizationInputs(
        AuthorizationState authzState, AuthorizationRequest request) {}

    @Override
    public AuthorizationDecision authorize(
        AuthorizationState authzState, AuthorizationRequest request) {
      return AuthorizationDecision.allow();
    }

    @Override
    public void authorizeOrThrow(
        PolarisPrincipal polarisPrincipal,
        Set<PolarisBaseEntity> activatedEntities,
        PolarisAuthorizableOperation authzOp,
        PolarisResolvedPathWrapper target,
        PolarisResolvedPathWrapper secondary) {}

    @Override
    public void authorizeOrThrow(
        PolarisPrincipal polarisPrincipal,
        Set<PolarisBaseEntity> activatedEntities,
        PolarisAuthorizableOperation authzOp,
      List<PolarisResolvedPathWrapper> targets,
      List<PolarisResolvedPathWrapper> secondaries) {}
  }
}
