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

package org.apache.polaris.service.identity.provider;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.service.identity.AwsIamServiceIdentityConfiguration;
import org.apache.polaris.service.identity.RealmServiceIdentityConfiguration;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@QuarkusTest
@TestProfile(DefaultServiceIdentityProviderTest.Profile.class)
public class DefaultServiceIdentityProviderTest {
  private static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  private static final String MY_REALM_KEY = "my-realm";

  @InjectMock RealmContext realmContext;
  @Inject ServiceIdentityConfiguration serviceIdentityConfiguration;
  @Inject ServiceIdentityCredentialCache credentialCache;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.identity-provider.type",
          "default",
          "polaris.service-identity.aws-iam.iam-arn",
          "arn:aws:iam::123456789012:user/polaris-default-iam-user",
          "polaris.service-identity.my-realm.aws-iam.iam-arn",
          "arn:aws:iam::123456789012:user/polaris-iam-user",
          "polaris.service-identity.my-realm.aws-iam.access-key-id",
          "access-key-id",
          "polaris.service-identity.my-realm.aws-iam.secret-access-key",
          "secret-access-key",
          "polaris.service-identity.my-realm.aws-iam.session-token",
          "session-token");
    }
  }

  @Test
  void testServiceIdentityConfiguration() {
    // Ensure that the service identity configuration is loaded correctly
    Assertions.assertThat(serviceIdentityConfiguration.realms()).isNotNull();
    Assertions.assertThat(serviceIdentityConfiguration.realms())
        .containsKey(ServiceIdentityConfiguration.DEFAULT_REALM_KEY)
        .containsKey(MY_REALM_KEY)
        .size()
        .isEqualTo(2);

    // Check the default realm configuration
    ServiceIdentityConfiguration.RealmConfigEntry defaultConfigEntry =
        serviceIdentityConfiguration.forRealm(DEFAULT_REALM_KEY);
    Assertions.assertThat(defaultConfigEntry.realm()).isEqualTo(DEFAULT_REALM_KEY);
    RealmServiceIdentityConfiguration defaultConfig = defaultConfigEntry.config();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().isPresent()).isTrue();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().accessKeyId()).isEmpty();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().secretAccessKey()).isEmpty();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().sessionToken()).isEmpty();

    // Check the my-realm configuration
    ServiceIdentityConfiguration.RealmConfigEntry myRealmConfigEntry =
        serviceIdentityConfiguration.forRealm(MY_REALM_KEY);
    Assertions.assertThat(myRealmConfigEntry.realm()).isEqualTo(MY_REALM_KEY);
    RealmServiceIdentityConfiguration myRealmConfig = myRealmConfigEntry.config();
    Assertions.assertThat(myRealmConfig.awsIamServiceIdentity().isPresent()).isTrue();
    Assertions.assertThat(myRealmConfig.awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");
    Assertions.assertThat(myRealmConfig.awsIamServiceIdentity().get().accessKeyId())
        .isEqualTo(Optional.of("access-key-id"));
    Assertions.assertThat(myRealmConfig.awsIamServiceIdentity().get().secretAccessKey())
        .isEqualTo(Optional.of("secret-access-key"));
    Assertions.assertThat(myRealmConfig.awsIamServiceIdentity().get().sessionToken())
        .isEqualTo(Optional.of("session-token"));

    // Check the unexisting realm configuration
    ServiceIdentityConfiguration.RealmConfigEntry otherConfigEntry =
        serviceIdentityConfiguration.forRealm("other-realm");
    Assertions.assertThat(otherConfigEntry.realm()).isEqualTo(DEFAULT_REALM_KEY);
    RealmServiceIdentityConfiguration otherConfig = otherConfigEntry.config();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().isPresent()).isTrue();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().accessKeyId()).isEmpty();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().secretAccessKey()).isEmpty();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().sessionToken()).isEmpty();
  }

  @Test
  void testAwsIamConfigurationLoading() {
    // Check the default realm
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider defaultProvider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    Optional<AwsIamServiceIdentityConfiguration> awsConfig =
        defaultProvider.getRealmConfig().awsIamServiceIdentity();
    Assertions.assertThat(awsConfig).isPresent();
    Assertions.assertThat(awsConfig.get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(awsConfig.get().accessKeyId()).isEmpty();

    // Check the my-realm with static credentials
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(MY_REALM_KEY);
    DefaultServiceIdentityProvider myRealmProvider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    awsConfig = myRealmProvider.getRealmConfig().awsIamServiceIdentity();
    Assertions.assertThat(awsConfig).isPresent();
    Assertions.assertThat(awsConfig.get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");
    Assertions.assertThat(awsConfig.get().accessKeyId()).isEqualTo(Optional.of("access-key-id"));
    Assertions.assertThat(awsConfig.get().secretAccessKey())
        .isEqualTo(Optional.of("secret-access-key"));
    Assertions.assertThat(awsConfig.get().sessionToken()).isEqualTo(Optional.of("session-token"));

    // Check the other realm (should fallback to default)
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn("other-realm");
    DefaultServiceIdentityProvider otherProvider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    awsConfig = otherProvider.getRealmConfig().awsIamServiceIdentity();
    Assertions.assertThat(awsConfig).isPresent();
    Assertions.assertThat(awsConfig.get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
  }

  @Test
  void testAllocateServiceIdentityWithSigV4Authentication() {
    // Test allocateServiceIdentity with SigV4 authentication
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider provider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://example.com/catalog")
            .setAuthenticationParameters(
                SigV4AuthenticationParameters.builder()
                    .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
                    .setRoleArn("arn:aws:iam::123456789012:role/customer-role")
                    .build())
            .build();

    Optional<ServiceIdentityInfoDpo> result = provider.allocateServiceIdentity(connectionConfig);

    Assertions.assertThat(result).isPresent();
    ServiceIdentityInfoDpo serviceIdentityDpo = result.get();
    Assertions.assertThat(serviceIdentityDpo).isInstanceOf(AwsIamServiceIdentityInfoDpo.class);
    Assertions.assertThat(serviceIdentityDpo.getIdentityType())
        .isEqualTo(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(serviceIdentityDpo.getIdentityInfoReference().urn())
        .contains("default-identity-provider");
  }

  @Test
  void testAllocateServiceIdentityWithBearerAuthenticationReturnsEmpty() {
    // Test allocateServiceIdentity with non-SigV4 authentication returns empty
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider provider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://example.com/catalog")
            .setAuthenticationParameters(
                BearerAuthenticationParameters.builder()
                    .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.BEARER)
                    .setBearerToken("some-token")
                    .build())
            .build();

    Optional<ServiceIdentityInfoDpo> result = provider.allocateServiceIdentity(connectionConfig);

    Assertions.assertThat(result).isEmpty();
  }

  @Test
  void testAllocateServiceIdentityWithNullAuthParametersReturnsEmpty() {
    // Test allocateServiceIdentity with null authentication parameters
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider provider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://example.com/catalog")
            .build();

    Optional<ServiceIdentityInfoDpo> result = provider.allocateServiceIdentity(connectionConfig);

    Assertions.assertThat(result).isEmpty();
  }

  @Test
  void testGetServiceIdentityInfoReturnsInfoWithoutCredentials() {
    // Test getServiceIdentityInfo returns user-facing info without credentials
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider provider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ServiceIdentityInfoDpo serviceIdentityDpo =
        new AwsIamServiceIdentityInfoDpo(
            new SecretReference(
                "urn:polaris-secret:default-identity-provider:system:default:AWS_IAM", Map.of()));

    Optional<ServiceIdentityInfo> result = provider.getServiceIdentityInfo(serviceIdentityDpo);

    Assertions.assertThat(result).isPresent();
    ServiceIdentityInfo info = result.get();
    Assertions.assertThat(info.getIdentityType())
        .isEqualTo(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM);
    Assertions.assertThat(info).isInstanceOf(AwsIamServiceIdentityInfo.class);
    AwsIamServiceIdentityInfo awsInfo = (AwsIamServiceIdentityInfo) info;
    Assertions.assertThat(awsInfo.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
  }

  @Test
  void testGetServiceIdentityCredentialReturnsCredentialWithSecrets() {
    // Test getServiceIdentityCredential returns full credential with secrets
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(MY_REALM_KEY);
    DefaultServiceIdentityProvider provider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ServiceIdentityInfoDpo serviceIdentityDpo =
        new AwsIamServiceIdentityInfoDpo(
            new SecretReference(
                "urn:polaris-secret:default-identity-provider:my-realm:AWS_IAM", Map.of()));

    Optional<ServiceIdentityCredential> result =
        provider.getServiceIdentityCredential(serviceIdentityDpo);

    Assertions.assertThat(result).isPresent();
    ServiceIdentityCredential credential = result.get();
    Assertions.assertThat(credential).isInstanceOf(AwsIamServiceIdentityCredential.class);

    AwsIamServiceIdentityCredential awsCredential = (AwsIamServiceIdentityCredential) credential;
    Assertions.assertThat(awsCredential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");
    Assertions.assertThat(awsCredential.getAwsCredentialsProvider())
        .isInstanceOf(StaticCredentialsProvider.class);

    // Verify credentials are accessible
    StaticCredentialsProvider credProvider =
        (StaticCredentialsProvider) awsCredential.getAwsCredentialsProvider();
    AwsSessionCredentials creds = (AwsSessionCredentials) credProvider.resolveCredentials();
    Assertions.assertThat(creds.accessKeyId()).isEqualTo("access-key-id");
    Assertions.assertThat(creds.secretAccessKey()).isEqualTo("secret-access-key");
    Assertions.assertThat(creds.sessionToken()).isEqualTo("session-token");
  }

  @Test
  void testGetServiceIdentityCredentialIsReusedAcrossRequests() {
    // A provider instance is created per request; the AWS credentials provider it hands out must be
    // shared so that the SDK's credential cache is not discarded after every request.
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider firstRequest =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);
    DefaultServiceIdentityProvider secondRequest =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    ServiceIdentityInfoDpo serviceIdentityDpo =
        new AwsIamServiceIdentityInfoDpo(
            DefaultServiceIdentityProvider.buildIdentityInfoReference(
                DEFAULT_REALM_KEY, ServiceIdentityType.AWS_IAM));

    AwsIamServiceIdentityCredential first =
        (AwsIamServiceIdentityCredential)
            firstRequest.getServiceIdentityCredential(serviceIdentityDpo).orElseThrow();
    AwsIamServiceIdentityCredential second =
        (AwsIamServiceIdentityCredential)
            secondRequest.getServiceIdentityCredential(serviceIdentityDpo).orElseThrow();

    Assertions.assertThat(first.getAwsCredentialsProvider())
        .isInstanceOf(DefaultCredentialsProvider.class);
    Assertions.assertThat(second.getAwsCredentialsProvider())
        .isSameAs(first.getAwsCredentialsProvider());
    // Only the provider is shared; each request gets its own credential object
    Assertions.assertThat(second).isNotSameAs(first);
  }

  @Test
  void testGetServiceIdentityCredentialSharesProviderOnlyAcrossRealmsWithTheSameIdentity() {
    ServiceIdentityInfoDpo defaultIdentity =
        new AwsIamServiceIdentityInfoDpo(
            DefaultServiceIdentityProvider.buildIdentityInfoReference(
                DEFAULT_REALM_KEY, ServiceIdentityType.AWS_IAM));
    ServiceIdentityInfoDpo myRealmIdentity =
        new AwsIamServiceIdentityInfoDpo(
            DefaultServiceIdentityProvider.buildIdentityInfoReference(
                MY_REALM_KEY, ServiceIdentityType.AWS_IAM));

    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    AwsCredentialsProvider defaultProvider =
        awsCredentialsProvider(
            new DefaultServiceIdentityProvider(
                realmContext, serviceIdentityConfiguration, credentialCache),
            defaultIdentity);

    // A realm without its own configuration falls back to the default identity and shares it
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn("other-realm");
    AwsCredentialsProvider otherRealmProvider =
        awsCredentialsProvider(
            new DefaultServiceIdentityProvider(
                realmContext, serviceIdentityConfiguration, credentialCache),
            defaultIdentity);

    // A realm with its own configuration gets its own provider
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(MY_REALM_KEY);
    AwsCredentialsProvider myRealmProvider =
        awsCredentialsProvider(
            new DefaultServiceIdentityProvider(
                realmContext, serviceIdentityConfiguration, credentialCache),
            myRealmIdentity);

    Assertions.assertThat(otherRealmProvider).isSameAs(defaultProvider);
    Assertions.assertThat(myRealmProvider)
        .isNotSameAs(defaultProvider)
        .isInstanceOf(StaticCredentialsProvider.class);
  }

  @Test
  void testCredentialCacheShutdownClosesAllProvidersEvenIfOneFails() {
    ServiceIdentityCredentialCache cache = new ServiceIdentityCredentialCache();
    AwsCredentialsProvider failingProvider = closeableProviderMock();
    AwsCredentialsProvider closeableProvider = closeableProviderMock();
    Mockito.doThrow(new IllegalStateException("close failed"))
        .when((SdkAutoCloseable) failingProvider)
        .close();

    cache.awsCredentialsProvider(DEFAULT_REALM_KEY, () -> failingProvider);
    cache.awsCredentialsProvider(MY_REALM_KEY, () -> closeableProvider);
    cache.shutdown();

    Mockito.verify((SdkAutoCloseable) failingProvider).close();
    Mockito.verify((SdkAutoCloseable) closeableProvider).close();
  }

  private static AwsCredentialsProvider awsCredentialsProvider(
      DefaultServiceIdentityProvider provider, ServiceIdentityInfoDpo serviceIdentity) {
    return ((AwsIamServiceIdentityCredential)
            provider.getServiceIdentityCredential(serviceIdentity).orElseThrow())
        .getAwsCredentialsProvider();
  }

  private static AwsCredentialsProvider closeableProviderMock() {
    return Mockito.mock(
        AwsCredentialsProvider.class,
        Mockito.withSettings().extraInterfaces(SdkAutoCloseable.class));
  }

  @Test
  void testEmptyProviderAllocateServiceIdentityReturnsEmpty() {
    // Test that an empty provider returns empty when allocating
    DefaultServiceIdentityProvider emptyProvider = new DefaultServiceIdentityProvider();

    ConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://example.com/catalog")
            .setAuthenticationParameters(
                SigV4AuthenticationParameters.builder()
                    .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
                    .setRoleArn("arn:aws:iam::123456789012:role/customer-role")
                    .build())
            .build();

    Optional<ServiceIdentityInfoDpo> result =
        emptyProvider.allocateServiceIdentity(connectionConfig);

    Assertions.assertThat(result).isEmpty();
  }

  @Test
  void testMultiTenantScenarioDifferentRealmsGetDifferentIdentities() {
    // Test that different realms have different configurations
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider defaultProvider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(MY_REALM_KEY);
    DefaultServiceIdentityProvider myRealmProvider =
        new DefaultServiceIdentityProvider(
            realmContext, serviceIdentityConfiguration, credentialCache);

    // Verify different IAM ARNs from configuration
    Assertions.assertThat(defaultProvider.getRealmConfig().awsIamServiceIdentity()).isPresent();
    Assertions.assertThat(defaultProvider.getRealmConfig().awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");

    Assertions.assertThat(myRealmProvider.getRealmConfig().awsIamServiceIdentity()).isPresent();
    Assertions.assertThat(myRealmProvider.getRealmConfig().awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");

    // Verify different credential configurations
    Assertions.assertThat(
            defaultProvider.getRealmConfig().awsIamServiceIdentity().get().accessKeyId())
        .isEmpty();
    Assertions.assertThat(
            myRealmProvider.getRealmConfig().awsIamServiceIdentity().get().accessKeyId())
        .isEqualTo(Optional.of("access-key-id"));
  }

  @Test
  void testBuildIdentityInfoReferenceForDefaultRealm() {
    // Test URN generation for default realm
    SecretReference ref =
        DefaultServiceIdentityProvider.buildIdentityInfoReference(
            DEFAULT_REALM_KEY, ServiceIdentityType.AWS_IAM);

    Assertions.assertThat(ref.urn())
        .isEqualTo("urn:polaris-secret:default-identity-provider:system:default:AWS_IAM");
  }

  @Test
  void testBuildIdentityInfoReferenceForCustomRealm() {
    // Test URN generation for custom realm
    SecretReference ref =
        DefaultServiceIdentityProvider.buildIdentityInfoReference(
            "custom-realm", ServiceIdentityType.AWS_IAM);

    Assertions.assertThat(ref.urn())
        .isEqualTo("urn:polaris-secret:default-identity-provider:custom-realm:AWS_IAM");
  }
}
