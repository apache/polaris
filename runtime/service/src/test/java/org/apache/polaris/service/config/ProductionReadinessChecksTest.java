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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.PrincipalMode;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksTest {

  private static final String REFLECTION_FREE_SERIALIZERS_PROPERTY =
      "quarkus.rest.jackson.optimization.enable-reflection-free-serializers";

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void reflectionFreeSerializersDisabledReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("false"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersUnsetReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers(null));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersEnabledReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("true"));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).isEqualTo(REFLECTION_FREE_SERIALIZERS_PROPERTY);
              assertThat(error.severe()).isTrue();
            });
  }

  @Test
  void externalPrincipalsWithExternalTypeReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipals(
            authenticationConfig(AuthenticationType.EXTERNAL, PrincipalMode.EXTERNAL));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsDisabledReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipals(
            authenticationConfig(AuthenticationType.INTERNAL, PrincipalMode.INTERNAL));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithInternalAuthenticationReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipals(
            authenticationConfig(AuthenticationType.INTERNAL, PrincipalMode.EXTERNAL));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.authentication.principal-mode");
              assertThat(error.severe()).isTrue();
            });
  }

  @ParameterizedTest
  @EnumSource(AuthenticationType.class)
  void internalPrincipalsWithInternalAuthorizerReturnsOk(AuthenticationType type) {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(type, PrincipalMode.INTERNAL), authorizationConfig("internal"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithNonInternalAuthorizerReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(AuthenticationType.EXTERNAL, PrincipalMode.EXTERNAL),
            authorizationConfig("ranger"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithInternalAuthorizerReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(AuthenticationType.EXTERNAL, PrincipalMode.EXTERNAL),
            authorizationConfig("internal"));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.authentication.principal-mode");
              assertThat(error.severe()).isTrue();
            });
  }

  private static AuthorizationConfiguration authorizationConfig(String type) {
    AuthorizationConfiguration config = mock(AuthorizationConfiguration.class);
    lenient().when(config.type()).thenReturn(type);
    return config;
  }

  private static AuthenticationConfiguration authenticationConfig(
      AuthenticationType type, PrincipalMode mode) {
    AuthenticationRealmConfiguration realmConfig = mock(AuthenticationRealmConfiguration.class);
    lenient().when(realmConfig.type()).thenReturn(type);
    lenient().when(realmConfig.principalMode()).thenReturn(mode);
    AuthenticationConfiguration config = mock(AuthenticationConfiguration.class);
    lenient()
        .when(config.realms())
        .thenReturn(Map.of(AuthenticationConfiguration.DEFAULT_REALM_KEY, realmConfig));
    return config;
  }

  private static Config configWithReflectionFreeSerializers(String value) {
    Config config = mock(Config.class);
    ConfigValue configValue = mock(ConfigValue.class);
    when(config.getConfigValue(REFLECTION_FREE_SERIALIZERS_PROPERTY)).thenReturn(configValue);
    when(configValue.getValue()).thenReturn(value);
    return config;
  }
}
