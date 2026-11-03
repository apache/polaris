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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.smallrye.common.annotation.Identifier;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.ImmutableAwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link R2LocalSigningCredentialVendingMechanism}: every {@code validate} rule, the rule
 * order, the token rule under both values of {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME}, and
 * {@code integrationFor}. The tokens are dummy values, not secrets.
 */
@SuppressWarnings("deprecation")
class R2LocalSigningCredentialVendingMechanismTest {

  private static final String ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";
  private static final String OTHER_ENDPOINT =
      "https://fedcba9876543210fedcba9876543210.r2.cloudflarestorage.com";
  private static final String FOR_MECHANISM =
      " for the R2_LOCAL_SIGNING credential vending mechanism";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String USER_ARN = "arn:aws:iam::123456789012:user/test-user";
  private static final String KMS_KEY = "arn:aws:kms:us-east-1:123456789012:key/test-key";

  private static final CloudflareR2ParentToken DEFAULT_TOKEN =
      new CloudflareR2ParentToken("test-r2-access-key", "test-r2-secret-key");
  private static final CloudflareR2ParentToken TEAM_TOKEN =
      new CloudflareR2ParentToken("test-r2-team-access-key", "test-r2-team-secret-key");

  /** A resolver that records every storage name it is asked for. A null name is the default. */
  private static final class RecordingResolver implements CloudflareR2ParentTokenResolver {
    private final Optional<CloudflareR2ParentToken> defaultToken;
    private final Map<String, CloudflareR2ParentToken> namedTokens;
    private final List<String> asked = new ArrayList<>();

    private RecordingResolver(
        Optional<CloudflareR2ParentToken> defaultToken,
        Map<String, CloudflareR2ParentToken> named) {
      this.defaultToken = defaultToken;
      this.namedTokens = named;
    }

    static RecordingResolver defaultOnly() {
      return new RecordingResolver(Optional.of(DEFAULT_TOKEN), Map.of());
    }

    static RecordingResolver namedOnly() {
      return new RecordingResolver(Optional.empty(), Map.of("team", TEAM_TOKEN));
    }

    static RecordingResolver defaultAndNamed() {
      return new RecordingResolver(Optional.of(DEFAULT_TOKEN), Map.of("team", TEAM_TOKEN));
    }

    static RecordingResolver none() {
      return new RecordingResolver(Optional.empty(), Map.of());
    }

    @Override
    public Optional<CloudflareR2ParentToken> resolve(String storageName) {
      asked.add(storageName);
      return storageName == null ? defaultToken : Optional.ofNullable(namedTokens.get(storageName));
    }
  }

  private static ImmutableAwsStorageConfigurationInfo.Builder r2Config() {
    return AwsStorageConfigurationInfo.builder()
        .credentialVendingMechanism(R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING)
        .endpoint(ENDPOINT)
        .pathStyleAccess(true)
        .region("auto")
        .addAllowedLocation("s3://bucket/wh/");
  }

  /** A stored config that belongs to some other mechanism, with fields R2 refuses on an update. */
  private static ImmutableAwsStorageConfigurationInfo.Builder otherMechanismConfig() {
    return AwsStorageConfigurationInfo.builder()
        .credentialVendingMechanism("OTHER_MECHANISM")
        .addAllowedLocation("s3://bucket/wh/");
  }

  private static RealmConfig realmConfig(boolean resolveByStorageName) {
    Map<String, Object> values =
        Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), resolveByStorageName);
    return new RealmConfigImpl((realmContext, name) -> values.get(name), () -> "test-realm");
  }

  private static R2LocalSigningCredentialVendingMechanism mechanism(
      CloudflareR2ParentTokenResolver resolver, RealmConfig realmConfig) {
    return new R2LocalSigningCredentialVendingMechanism(
        resolver, Clock.systemUTC(), null, realmConfig);
  }

  /** A mechanism with the default token and the flag false. */
  private static R2LocalSigningCredentialVendingMechanism defaultMechanism() {
    return mechanism(RecordingResolver.defaultOnly(), realmConfig(false));
  }

  private static String refusalMessage(
      R2LocalSigningCredentialVendingMechanism mechanism,
      AwsStorageConfigurationInfo current,
      AwsStorageConfigurationInfo updated) {
    Throwable thrown = catchThrowable(() -> mechanism.validate(current, updated));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
    return thrown.getMessage();
  }

  private static String endpointMessage(String endpoint) {
    return catchThrowable(() -> CloudflareR2Endpoint.parse(endpoint)).getMessage();
  }

  // ---------------------------------------------------------------- identity and integration

  @Test
  void identifierIsR2LocalSigning() {
    Identifier identifier =
        R2LocalSigningCredentialVendingMechanism.class.getAnnotation(Identifier.class);

    assertThat(R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING)
        .isEqualTo("R2_LOCAL_SIGNING");
    assertThat(identifier.value()).isEqualTo("R2_LOCAL_SIGNING");
  }

  @Test
  void integrationForReturnsAnR2IntegrationBoundToTheConfigAndTheRealmConfig() {
    RealmConfig realmConfig = realmConfig(false);
    R2LocalSigningCredentialVendingMechanism mechanism =
        mechanism(RecordingResolver.defaultOnly(), realmConfig);
    AwsStorageConfigurationInfo config = r2Config().build();

    PolarisStorageIntegration integration = mechanism.integrationFor(config);

    assertThat(integration).isInstanceOf(CloudflareR2CredentialsStorageIntegration.class);
    CloudflareR2CredentialsStorageIntegration r2 =
        (CloudflareR2CredentialsStorageIntegration) integration;
    assertThat(r2.storageConfig()).isSameAs(config);
    assertThat(r2.realmConfig()).isSameAs(realmConfig);
  }

  @Test
  void integrationForBuildsANewIntegrationOnEachCall() {
    R2LocalSigningCredentialVendingMechanism mechanism = defaultMechanism();
    AwsStorageConfigurationInfo config = r2Config().build();

    assertThat(mechanism.integrationFor(config)).isNotSameAs(mechanism.integrationFor(config));
  }

  // ---------------------------------------------------------------- accepted configs

  @Test
  void acceptsAValidConfigAtCreate() {
    assertThatCode(() -> defaultMechanism().validate(null, r2Config().build()))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsExplicitFalseForStsUnavailableAndKmsUnavailable() {
    assertThatCode(
            () ->
                defaultMechanism()
                    .validate(null, r2Config().stsUnavailable(false).kmsUnavailable(false).build()))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsEmptyKmsKeyLists() {
    AwsStorageConfigurationInfo updated =
        r2Config()
            .allowedKmsKeys(List.of())
            .encryptionKeys(List.of())
            .decryptionKeys(List.of())
            .build();

    assertThatCode(() -> defaultMechanism().validate(null, updated)).doesNotThrowAnyException();
  }

  @Test
  void acceptsAnUpdateWithAnUnchangedConfig() {
    assertThatCode(() -> defaultMechanism().validate(r2Config().build(), r2Config().build()))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsACurrentConfigThatCarriesAnotherMechanism() {
    AwsStorageConfigurationInfo current =
        otherMechanismConfig().roleARN(ROLE_ARN).endpoint(ENDPOINT).build();
    AwsStorageConfigurationInfo noIdentifier =
        AwsStorageConfigurationInfo.builder()
            .endpoint(ENDPOINT)
            .addAllowedLocation("s3://bucket/wh/")
            .build();

    assertThatCode(() -> defaultMechanism().validate(current, r2Config().build()))
        .doesNotThrowAnyException();
    assertThatCode(() -> defaultMechanism().validate(noIdentifier, r2Config().build()))
        .doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------- rule 1: endpoint

  @Test
  void refusesAMissingEndpoint() {
    assertThatThrownBy(() -> defaultMechanism().validate(null, r2Config().endpoint(null).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(endpointMessage(null))
        .hasMessageContaining(
            "endpoint is required for the R2_LOCAL_SIGNING credential vending mechanism");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://s3.us-east-1.amazonaws.com",
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/",
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com:443",
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/path",
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com?x=1",
        "https://0123456789ABCDEF0123456789ABCDEF.r2.cloudflarestorage.com"
      })
  void refusesAnEndpointThatIsNotAnR2Endpoint(String endpoint) {
    assertThatThrownBy(
            () -> defaultMechanism().validate(null, r2Config().endpoint(endpoint).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(endpointMessage(endpoint))
        .hasMessageContaining("is not a Cloudflare R2 endpoint");
  }

  // ---------------------------------------------------------------- rule 2: path style, region

  @Test
  void refusesPathStyleAccessThatIsNotTrue() {
    String expected = "pathStyleAccess must be true" + FOR_MECHANISM;

    assertThatThrownBy(
            () -> defaultMechanism().validate(null, r2Config().pathStyleAccess(false).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
    assertThatThrownBy(
            () -> defaultMechanism().validate(null, r2Config().pathStyleAccess(null).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
  }

  @Test
  void refusesARegionThatIsNotAuto() {
    String expected = "region must be 'auto'" + FOR_MECHANISM;

    assertThatThrownBy(
            () -> defaultMechanism().validate(null, r2Config().region("us-east-1").build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
    assertThatThrownBy(() -> defaultMechanism().validate(null, r2Config().region(null).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
  }

  // ---------------------------------------------------------------- rule 3: absent fields

  private static Arguments forbidden(
      String field,
      String verb,
      UnaryOperator<ImmutableAwsStorageConfigurationInfo.Builder> change) {
    return refused(field, field + " must not be " + verb + FOR_MECHANISM, change);
  }

  private static Arguments refused(
      String name,
      String expected,
      UnaryOperator<ImmutableAwsStorageConfigurationInfo.Builder> change) {
    return Arguments.of(name, expected, change);
  }

  static Stream<Arguments> forbiddenFields() {
    return Stream.of(
        forbidden("roleArn", "set", b -> b.roleARN(ROLE_ARN)),
        forbidden("externalId", "set", b -> b.externalId("test-external-id")),
        forbidden("userArn", "set", b -> b.userARN(USER_ARN)),
        forbidden("stsEndpoint", "set", b -> b.stsEndpoint("https://sts.example.com")),
        forbidden("stsUnavailable", "true", b -> b.stsUnavailable(true)),
        forbidden("endpointInternal", "set", b -> b.endpointInternal(ENDPOINT)),
        forbidden("kmsUnavailable", "true", b -> b.kmsUnavailable(true)),
        forbidden("currentKmsKey", "set", b -> b.currentKmsKey(KMS_KEY)),
        forbidden("allowedKmsKeys", "set", b -> b.allowedKmsKeys(List.of(KMS_KEY))),
        // Core stores the request's currentKmsKey and allowedKmsKeys as encryptionKeys, so the
        // message names all three request fields.
        refused(
            "encryptionKeys",
            "encryptionKeys, currentKmsKey, and allowedKmsKeys must not be set" + FOR_MECHANISM,
            b -> b.encryptionKeys(List.of(KMS_KEY))),
        forbidden("decryptionKeys", "set", b -> b.decryptionKeys(List.of(KMS_KEY))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("forbiddenFields")
  void refusesAFieldThatMustBeAbsent(
      String field,
      String expected,
      UnaryOperator<ImmutableAwsStorageConfigurationInfo.Builder> change) {
    AwsStorageConfigurationInfo updated = change.apply(r2Config()).build();

    assertThatThrownBy(() -> defaultMechanism().validate(null, updated))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
  }

  // ---------------------------------------------------------------- rule 4: doubled slashes

  @ParameterizedTest
  @ValueSource(strings = {"s3://b//", "s3://b//x/", "s3://b/x//y/", "s3a://b/x//y/"})
  void refusesAnAllowedLocationWithAnEmptyPathSegment(String location) {
    AwsStorageConfigurationInfo updated = r2Config().allowedLocations(List.of(location)).build();

    assertThatThrownBy(() -> defaultMechanism().validate(null, updated))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "allowed location '"
                + location
                + "' contains an empty path segment ('//'), which the R2_LOCAL_SIGNING"
                + " credential vending mechanism does not accept");
  }

  @Test
  void namesTheOffendingLocationWhenOnlyOneOfSeveralIsBad() {
    AwsStorageConfigurationInfo updated =
        r2Config().allowedLocations(List.of("s3://b/ok/", "s3://b/bad//x/")).build();

    assertThat(refusalMessage(defaultMechanism(), null, updated)).contains("'s3://b/bad//x/'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://b", "s3://b/", "s3://b/x/", "s3a://b/x/y/"})
  void acceptsAnAllowedLocationWithSingleSlashes(String location) {
    AwsStorageConfigurationInfo updated = r2Config().allowedLocations(List.of(location)).build();

    assertThatCode(() -> defaultMechanism().validate(null, updated)).doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------- endpoint changes on update

  @Test
  void acceptsAnEndpointChangeOnUpdate() {
    AwsStorageConfigurationInfo updated = r2Config().endpoint(OTHER_ENDPOINT).build();

    assertThatCode(() -> defaultMechanism().validate(r2Config().build(), updated))
        .doesNotThrowAnyException();
    assertThatCode(() -> defaultMechanism().validate(otherMechanismConfig().build(), updated))
        .doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------- rule 5: parent token

  @Test
  void withTheFlagFalseTheStorageNameIsIgnoredAndTheDefaultTokenIsUsed() {
    RecordingResolver resolver = RecordingResolver.defaultOnly();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(false));

    mechanism.validate(null, r2Config().storageName("team").build());

    assertThat(resolver.asked).hasSize(1).containsOnlyNulls();
  }

  @Test
  void withTheFlagFalseANamedTokenDoesNotReplaceAMissingDefaultToken() {
    RecordingResolver resolver = RecordingResolver.namedOnly();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(false));
    AwsStorageConfigurationInfo updated = r2Config().storageName("team").build();

    String message = refusalMessage(mechanism, null, updated);

    assertThat(message).isEqualTo(CloudflareR2ParentTokenResolver.missingTokenMessage(null));
    assertThat(message).contains("polaris.storage.cloudflare-r2.access-key");
    assertThat(resolver.asked).hasSize(1).containsOnlyNulls();
  }

  @Test
  void withTheFlagTrueANamedCatalogUsesItsNamedToken() {
    RecordingResolver resolver = RecordingResolver.defaultAndNamed();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(true));

    mechanism.validate(null, r2Config().storageName("team").build());

    assertThat(resolver.asked).containsExactly("team");
  }

  @Test
  void withTheFlagTrueAnUnknownStorageNameIsRefusedWithoutFallingBackToTheDefault() {
    RecordingResolver resolver = RecordingResolver.defaultOnly();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(true));
    AwsStorageConfigurationInfo updated = r2Config().storageName("team").build();

    String message = refusalMessage(mechanism, null, updated);

    assertThat(message).isEqualTo(CloudflareR2ParentTokenResolver.missingTokenMessage("team"));
    assertThat(message).contains("polaris.storage.cloudflare-r2.team.access-key");
    assertThat(message.contains(DEFAULT_TOKEN.secretAccessKey())).as("secret in message").isFalse();
    assertThat(resolver.asked).containsExactly("team");
  }

  @Test
  void withTheFlagTrueACatalogWithoutAStorageNameUsesTheDefaultToken() {
    RecordingResolver resolver = RecordingResolver.defaultAndNamed();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(true));

    mechanism.validate(null, r2Config().build());

    assertThat(resolver.asked).hasSize(1).containsOnlyNulls();
  }

  @Test
  void withTheFlagTrueACatalogWithoutAStorageNameAndWithoutADefaultTokenIsRefused() {
    R2LocalSigningCredentialVendingMechanism mechanism =
        mechanism(RecordingResolver.namedOnly(), realmConfig(true));

    assertThat(refusalMessage(mechanism, null, r2Config().build()))
        .isEqualTo(CloudflareR2ParentTokenResolver.missingTokenMessage(null));
  }

  // ---------------------------------------------------------------- rule order

  @Test
  void theEndpointRuleRunsBeforeTheTokenRule() {
    RecordingResolver resolver = RecordingResolver.none();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(resolver, realmConfig(false));
    String badEndpoint = "https://example.com";

    String message = refusalMessage(mechanism, null, r2Config().endpoint(badEndpoint).build());

    assertThat(message).isEqualTo(endpointMessage(badEndpoint));
    assertThat(resolver.asked).isEmpty();
  }

  @Test
  void everyRuleRunsBeforeTheRuleAfterIt() {
    RecordingResolver none = RecordingResolver.none();
    R2LocalSigningCredentialVendingMechanism mechanism = mechanism(none, realmConfig(false));
    String badEndpoint = "https://example.com";

    // Rule 1 before rule 2.
    assertThat(
            refusalMessage(
                mechanism, null, r2Config().endpoint(badEndpoint).pathStyleAccess(false).build()))
        .isEqualTo(endpointMessage(badEndpoint));
    // Path style before region, inside rule 2.
    assertThat(
            refusalMessage(
                mechanism, null, r2Config().pathStyleAccess(false).region("us-east-1").build()))
        .isEqualTo("pathStyleAccess must be true" + FOR_MECHANISM);
    // Rule 2 before rule 3.
    assertThat(
            refusalMessage(
                mechanism, null, r2Config().pathStyleAccess(false).roleARN(ROLE_ARN).build()))
        .isEqualTo("pathStyleAccess must be true" + FOR_MECHANISM);
    // Rule 3 before rule 4.
    assertThat(
            refusalMessage(
                mechanism,
                null,
                r2Config().roleARN(ROLE_ARN).allowedLocations(List.of("s3://b//x/")).build()))
        .isEqualTo("roleArn must not be set" + FOR_MECHANISM);
    // Rule 4 before rule 5: the resolver finds no token, but rule 4 reports first.
    assertThat(
            refusalMessage(
                mechanism, null, r2Config().allowedLocations(List.of("s3://b//x/")).build()))
        .contains("contains an empty path segment ('//')");
    assertThat(none.asked).isEmpty();
  }
}
