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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.CredentialMode;
import org.apache.polaris.service.auth.external.OidcConfiguration;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.apache.polaris.service.storage.S3CredentialVendingMechanisms;
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
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsDisabledReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipals(
            authenticationConfig(AuthenticationType.INTERNAL, CredentialMode.INTERNAL));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithInternalAuthenticationReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipals(
            authenticationConfig(AuthenticationType.INTERNAL, CredentialMode.EXTERNAL));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.authentication.credential-mode");
              assertThat(error.severe()).isTrue();
            });
  }

  @ParameterizedTest
  @EnumSource(AuthenticationType.class)
  void internalPrincipalsWithInternalAuthorizerReturnsOk(AuthenticationType type) {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(type, CredentialMode.INTERNAL), authorizationConfig("internal"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithNonInternalAuthorizerReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            authorizationConfig("ranger"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void externalPrincipalsWithInternalAuthorizerReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkExternalPrincipalsAuthorizer(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            authorizationConfig("internal"));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.authentication.credential-mode");
              assertThat(error.severe()).isTrue();
            });
  }

  @Test
  void oidcMappingWithInternalAuthTypeReturnsOk() {
    // OIDC is not involved; the check should be skipped regardless of claim-path config
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.INTERNAL, CredentialMode.INTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.empty(),
                Optional.empty()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void oidcMappingExternalModeWithNameClaimPathReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.of("preferred_username"),
                Optional.empty()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void oidcMappingExternalModeWithoutNameClaimPathReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.empty(),
                Optional.empty()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.oidc.principal-mapper.name-claim-path");
              assertThat(error.severe()).isTrue();
            });
  }

  @Test
  void oidcMappingExternalModeWithIdClaimPathPresentReturnsWarning() {
    // name-claim-path is set (required) but id-claim-path is also set (ignored in external mode)
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.of("preferred_username"),
                Optional.of("sub")));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.oidc.principal-mapper.id-claim-path");
              assertThat(error.severe()).isFalse();
            });
  }

  @Test
  void oidcMappingInternalModeWithNameClaimPathReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.INTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.of("preferred_username"),
                Optional.empty()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void oidcMappingInternalModeWithIdClaimPathReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.INTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.empty(),
                Optional.of("sub")));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void oidcMappingInternalModeWithoutAnyPathReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.INTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.empty(),
                Optional.empty()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.oidc.principal-mapper.name-claim-path");
              assertThat(error.severe()).isTrue();
            });
  }

  @Test
  void oidcMappingNamedTenantWithoutNameClaimPathReturnsWarningNotSevere() {
    // Named tenants are only activated at runtime; misconfiguration is a warning, not a blocker
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            oidcConfig("idp1", "default", Optional.empty(), Optional.empty()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.oidc.idp1.principal-mapper.name-claim-path");
              assertThat(error.severe()).isFalse();
            });
  }

  @Test
  void oidcMappingWithCustomMapperTypeSkipsValidation() {
    // Custom mappers handle their own name resolution; no check is applied
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.EXTERNAL, CredentialMode.EXTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "custom",
                Optional.empty(),
                Optional.empty()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void oidcMappingMixedAuthTypeExternalModeWithoutNameClaimPathReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkOidcPrincipalMapping(
            authenticationConfig(AuthenticationType.MIXED, CredentialMode.EXTERNAL),
            oidcConfig(
                OidcConfiguration.DEFAULT_TENANT_KEY,
                "default",
                Optional.empty(),
                Optional.empty()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.oidc.principal-mapper.name-claim-path");
              assertThat(error.severe()).isTrue();
            });
  }

  private static OidcConfiguration oidcConfig(
      String tenantId,
      String mapperType,
      Optional<String> nameClaimPath,
      Optional<String> idClaimPath) {
    OidcTenantConfiguration.PrincipalMapper pm =
        mock(OidcTenantConfiguration.PrincipalMapper.class);
    lenient().when(pm.type()).thenReturn(mapperType);
    lenient().when(pm.nameClaimPath()).thenReturn(nameClaimPath);
    lenient().when(pm.idClaimPath()).thenReturn(idClaimPath);
    OidcTenantConfiguration tenant = mock(OidcTenantConfiguration.class);
    lenient().when(tenant.principalMapper()).thenReturn(pm);
    OidcConfiguration config = mock(OidcConfiguration.class);
    lenient().when(config.tenants()).thenReturn(Map.of(tenantId, tenant));
    return config;
  }

  private static AuthorizationConfiguration authorizationConfig(String type) {
    AuthorizationConfiguration config = mock(AuthorizationConfiguration.class);
    lenient().when(config.type()).thenReturn(type);
    return config;
  }

  private static AuthenticationConfiguration authenticationConfig(
      AuthenticationType type, CredentialMode mode) {
    AuthenticationRealmConfiguration realmConfig = mock(AuthenticationRealmConfiguration.class);
    lenient().when(realmConfig.type()).thenReturn(type);
    lenient().when(realmConfig.credentialMode()).thenReturn(mode);
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

  private static final String MECHANISMS_KEY =
      FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS.key();

  private static S3CredentialVendingMechanisms installed(String... ids) {
    Map<String, S3CredentialVendingMechanism> mechanisms = new HashMap<>();
    for (String id : ids) {
      mechanisms.put(id, mock(S3CredentialVendingMechanism.class));
    }
    return new S3CredentialVendingMechanisms(mechanisms);
  }

  private static FeaturesConfiguration featuresConfig(
      Map<String, String> defaults, Map<String, RealmOverridable.RealmOverrides> realmOverrides) {
    // CALLS_REAL_METHODS keeps the interface's default parseDefaults/parseRealmOverrides working.
    FeaturesConfiguration config =
        mock(FeaturesConfiguration.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(config.defaults()).thenReturn(defaults);
    when(config.realmOverrides()).thenReturn(realmOverrides);
    return config;
  }

  private static RealmOverridable.RealmOverrides overrides(Map<String, String> values) {
    RealmOverridable.RealmOverrides realmOverrides = mock(RealmOverridable.RealmOverrides.class);
    when(realmOverrides.overrides()).thenReturn(values);
    return realmOverrides;
  }

  @Test
  void everyAllowlistedMechanismInstalledIsReady() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(
                Map.of(MECHANISMS_KEY, "[\"STS\"]"),
                Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"STS\"]")))),
            installed("STS", "DEFAULT"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInDefaultsIsNonSevereAndNamesWhatIsAvailable() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\",\"BOGUS\"]"), Map.of()),
            installed("STS", "DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
              assertThat(error.message()).contains("BOGUS").contains("STS");
            });
  }

  @Test
  void anUnconfiguredAllowlistChecksTheCodeDefault() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of()), installed("DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
              assertThat(error.message()).contains("STS");
            });
  }

  @Test
  void anUnconfiguredAllowlistIsReadyWhenTheDefaultMechanismIsInstalled() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of()), installed("STS", "DEFAULT"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInARealmOverrideNamesTheRealm() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"NOPE\"]")))),
            installed("STS", "DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty()).contains("r1").contains(MECHANISMS_KEY);
              assertThat(error.message()).contains("NOPE").contains("STS");
            });
  }

  @Test
  void aMissingDefaultMechanismIsASevereError() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\"]"), Map.of()), installed("STS"));
    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isTrue();
              assertThat(error.message()).contains("DEFAULT");
              assertThat(error.offendingProperty())
                  .isEqualTo("S3CredentialVendingMechanism @Identifier(\"DEFAULT\")");
            });
  }

  @Test
  void aListedDefaultIsANonSevereWarningEvenThoughItIsInstalled() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\",\"DEFAULT\"]"), Map.of()),
            installed("STS", "DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.message()).contains("DEFAULT").contains("has no effect");
            });
  }
}
