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
package org.apache.polaris.extension.auth.opa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

class OpaProductionReadinessChecksTest {

  @Test
  void checkOpaAuthorizationIsOkWhenOpaIsNotConfigured() {
    @SuppressWarnings("unchecked")
    Instance<OpaPolarisAuthorizerFactory> opaAuthorizerFactories = mock(Instance.class);

    ProductionReadinessCheck check =
        new OpaProductionReadinessChecks()
            .checkOpaAuthorization(
                config(Map.of("polaris.authorization.type", "internal")), opaAuthorizerFactories);

    assertThat(check.ready()).isTrue();
    verifyNoInteractions(opaAuthorizerFactories);
  }

  @Test
  void checkOpaAuthorizationReportsEveryConfiguredOpaRealm() {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("polaris.authorization.type", "opa");
    properties.put("polaris.authorization.realms.realm1.type", "opa");
    properties.put("polaris.authorization.realms.realm2.type", "opa");
    properties.put("polaris.authorization.realms.realm3.type", "internal");

    OpaAuthorizationConfig opaConfig = mock(OpaAuthorizationConfig.class);
    OpaAuthorizationConfig.HttpConfig httpConfig = mock(OpaAuthorizationConfig.HttpConfig.class);
    when(opaConfig.http()).thenReturn(httpConfig);
    when(httpConfig.verifySsl()).thenReturn(false);

    ProductionReadinessCheck check =
        new OpaProductionReadinessChecks()
            .checkOpaAuthorization(config(properties), opaAuthorizerFactories(opaConfig));

    assertThat(check.getErrors())
        .extracting(ProductionReadinessCheck.Error::offendingProperty)
        .containsExactly(
            "polaris.authorization.type",
            "polaris.authorization.realms.realm1.type",
            "polaris.authorization.realms.realm2.type",
            "polaris.authorization.opa.http.verify-ssl");
  }

  private static Config config(Map<String, String> properties) {
    Config config = mock(Config.class);
    when(config.getPropertyNames()).thenReturn(properties.keySet());
    when(config.getOptionalValue(anyString(), eq(String.class)))
        .thenAnswer(invocation -> Optional.ofNullable(properties.get(invocation.getArgument(0))));
    return config;
  }

  private static Instance<OpaPolarisAuthorizerFactory> opaAuthorizerFactories(
      OpaAuthorizationConfig opaConfig) {
    @SuppressWarnings("unchecked")
    Instance<OpaPolarisAuthorizerFactory> opaAuthorizerFactories = mock(Instance.class);
    OpaPolarisAuthorizerFactory opaAuthorizerFactory = mock(OpaPolarisAuthorizerFactory.class);
    when(opaAuthorizerFactories.select(Identifier.Literal.of("opa")))
        .thenReturn(opaAuthorizerFactories);
    when(opaAuthorizerFactories.isResolvable()).thenReturn(true);
    when(opaAuthorizerFactories.get()).thenReturn(opaAuthorizerFactory);
    when(opaAuthorizerFactory.getConfig()).thenReturn(opaConfig);
    return opaAuthorizerFactories;
  }
}
