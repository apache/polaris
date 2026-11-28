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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.inject.Instance;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.CredentialMode;
import org.apache.polaris.service.auth.external.OidcConfiguration;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.apache.polaris.service.credentials.connection.AuthType;
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
        .satisfies(error -> {
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
        .satisfies(error -> {
          assertThat(error.offendingProperty()).isEqualTo("polaris.authentication.credential-mode");
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
        .satisfies(error -> {
          assertThat(error.offendingProperty()).isEqualTo("polaris.authentication.credential-mode");
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
        .satisfies(error -> {
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
        .satisfies(error -> {
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
        .satisfies(error -> {
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
        .satisfies(error -> {
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
        .satisfies(error -> {
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

  /** The configuration source the server builds from the same feature configuration. */
  private static RealmConfigurationSource configurationSource(FeaturesConfiguration config) {
    return new DefaultConfigurationStore(new ObjectMapper(), config);
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
    FeaturesConfiguration config =
        featuresConfig(
            Map.of(MECHANISMS_KEY, "[\"STS\"]"),
            Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"STS\"]"))));
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("STS"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInDefaultsIsNonSevereAndNamesWhatIsAvailable() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\",\"BOGUS\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("STS"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isFalse();
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
          assertThat(error.message()).contains("BOGUS").contains("STS");
        });
  }

  @Test
  void anUnconfiguredAllowlistChecksTheCodeDefault() {
    FeaturesConfiguration config = featuresConfig(Map.of(), Map.of());
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("SECOND"));
    assertThat(result.getErrors())
        .filteredOn(error -> !error.severe())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
          assertThat(error.message()).contains("STS");
        });
  }

  @Test
  void anUnconfiguredAllowlistIsReadyWhenStsIsInstalled() {
    FeaturesConfiguration config = featuresConfig(Map.of(), Map.of());
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("STS"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInARealmOverrideNamesTheRealm() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"NOPE\"]"))));
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("STS"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isFalse();
          assertThat(error.offendingProperty()).contains("r1").contains(MECHANISMS_KEY);
          assertThat(error.message()).contains("NOPE").contains("STS");
        });
  }

  @Test
  void aMissingStsMechanismIsASevereError() {
    FeaturesConfiguration config = featuresConfig(Map.of(MECHANISMS_KEY, "[\"SECOND\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            config, configurationSource(config), installed("SECOND"));
    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("STS");
          assertThat(error.offendingProperty())
              .isEqualTo("S3CredentialVendingMechanism @Identifier(\"STS\")");
        });
  }

  private static final String INSECURE_STORAGE_KEY =
      FeatureConfiguration.ALLOW_INSECURE_STORAGE_TYPES.key();
  private static final String STORAGE_TYPES_KEY =
      FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES.key();

  @Test
  void unconfiguredStorageSettingsAreReady() {
    FeaturesConfiguration config = featuresConfig(Map.of(), Map.of());
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void insecureStorageTypesAllowedInDefaultsIsSevere() {
    FeaturesConfiguration config = featuresConfig(Map.of(INSECURE_STORAGE_KEY, "true"), Map.of());
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + INSECURE_STORAGE_KEY + "\"");
        });
  }

  @Test
  void insecureStorageTypesAllowedAsAJsonStringIsSevere() {
    // The runtime converts the JSON string "true" to the flag's boolean type, so the flag is on.
    FeaturesConfiguration config =
        featuresConfig(Map.of(INSECURE_STORAGE_KEY, "\"true\""), Map.of());
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + INSECURE_STORAGE_KEY + "\"");
        });
  }

  @Test
  void insecureStorageTypesAllowedInARealmOverrideNamesTheRealm() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(INSECURE_STORAGE_KEY, "true"))));
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.offendingProperty())
              .isEqualTo(
                  "polaris.features.realm-overrides.\"r1\".overrides.\""
                      + INSECURE_STORAGE_KEY
                      + "\"");
        });
  }

  @Test
  void anInsecureSupportedStorageTypeInDefaultsIsSevere() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(STORAGE_TYPES_KEY, "[\"S3\",\"FILE\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("'FILE'");
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + STORAGE_TYPES_KEY + "\"");
        });
  }

  @Test
  void anInsecureSupportedStorageTypeInARealmOverrideNamesTheRealm() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(STORAGE_TYPES_KEY, "[\"FILE\"]"))));
    ProductionReadinessCheck result =
        checks.checkInsecureStorageSettings(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("'FILE'");
          assertThat(error.offendingProperty())
              .isEqualTo(
                  "polaris.features.realm-overrides.\"r1\".overrides.\""
                      + STORAGE_TYPES_KEY
                      + "\"");
        });
  }

  private static final String SKIP_SUBSCOPING_KEY =
      FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION.key();

  @Test
  void credentialSubscopingNotSkippedIsReady() {
    FeaturesConfiguration config = featuresConfig(Map.of(), Map.of());
    ProductionReadinessCheck result =
        checks.checkSkipCredentialSubscopingIndirection(config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void skippingCredentialSubscopingInDefaultsIsAWarning() {
    FeaturesConfiguration config = featuresConfig(Map.of(SKIP_SUBSCOPING_KEY, "true"), Map.of());
    ProductionReadinessCheck result =
        checks.checkSkipCredentialSubscopingIndirection(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isFalse();
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + SKIP_SUBSCOPING_KEY + "\"");
        });
  }

  @Test
  void skippingCredentialSubscopingInARealmOverrideNamesTheRealm() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(SKIP_SUBSCOPING_KEY, "true"))));
    ProductionReadinessCheck result =
        checks.checkSkipCredentialSubscopingIndirection(config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isFalse();
          assertThat(error.offendingProperty())
              .isEqualTo(
                  "polaris.features.realm-overrides.\"r1\".overrides.\""
                      + SKIP_SUBSCOPING_KEY
                      + "\"");
        });
  }

  private static final String FEDERATION_KEY = FeatureConfiguration.ENABLE_CATALOG_FEDERATION.key();
  private static final String AUTH_TYPES_KEY =
      FeatureConfiguration.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES.key();

  /** Credential vendors for exactly the given authentication types. */
  @SuppressWarnings("unchecked")
  private static Instance<ConnectionCredentialVendor> vendorsFor(String... authTypes) {
    Set<String> installed = Set.of(authTypes);
    Instance<ConnectionCredentialVendor> vendors = mock(Instance.class);
    lenient()
        .when(vendors.select(any(Annotation[].class)))
        .thenAnswer(invocation -> {
          AuthType qualifier = invocation.getArgument(0);
          Instance<ConnectionCredentialVendor> selected = mock(Instance.class);
          when(selected.isUnsatisfied()).thenReturn(!installed.contains(qualifier.value().name()));
          return selected;
        });
    return vendors;
  }

  @Test
  void federationDisabledChecksNoCredentialVendor() {
    FeaturesConfiguration config = featuresConfig(Map.of(AUTH_TYPES_KEY, "[\"OAUTH\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(vendorsFor(), config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void federationWithAVendorForEachConfiguredTypeIsReady() {
    FeaturesConfiguration config =
        featuresConfig(
            Map.of(FEDERATION_KEY, "true", AUTH_TYPES_KEY, "[\"OAUTH\",\"BEARER\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(
            vendorsFor("OAUTH", "BEARER"), config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void federationWithoutAVendorForAConfiguredTypeIsSevere() {
    FeaturesConfiguration config =
        featuresConfig(
            Map.of(FEDERATION_KEY, "true", AUTH_TYPES_KEY, "[\"OAUTH\",\"BEARER\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(
            vendorsFor("OAUTH"), config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("'BEARER'");
          assertThat(error.offendingProperty())
              .isEqualTo("polaris.features.\"" + AUTH_TYPES_KEY + "\"");
        });
  }

  @Test
  void federationWithoutConfiguredTypesChecksNoCredentialVendor() {
    FeaturesConfiguration config = featuresConfig(Map.of(FEDERATION_KEY, "true"), Map.of());
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(vendorsFor(), config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void aRealmEnablingFederationIsCheckedWithTheDefaultAuthenticationTypes() {
    FeaturesConfiguration config =
        featuresConfig(
            Map.of(AUTH_TYPES_KEY, "[\"OAUTH\"]"),
            Map.of("r1", overrides(Map.of(FEDERATION_KEY, "true"))));
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(vendorsFor(), config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("'OAUTH'");
        });
  }

  @Test
  void aRealmDisablingFederationIsNotChecked() {
    FeaturesConfiguration config =
        featuresConfig(
            Map.of(FEDERATION_KEY, "true", AUTH_TYPES_KEY, "[\"OAUTH\"]"),
            Map.of(
                "r1", overrides(Map.of(FEDERATION_KEY, "false", AUTH_TYPES_KEY, "[\"BEARER\"]"))));
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(
            vendorsFor("OAUTH"), config, configurationSource(config));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anUnknownConfiguredAuthenticationTypeIsSevere() {
    FeaturesConfiguration config =
        featuresConfig(Map.of(FEDERATION_KEY, "true", AUTH_TYPES_KEY, "[\"NOPE\"]"), Map.of());
    ProductionReadinessCheck result =
        checks.checkConnectionCredentialVendors(vendorsFor(), config, configurationSource(config));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.severe()).isTrue();
          assertThat(error.message()).contains("'NOPE'");
        });
  }
}
