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
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.apache.polaris.service.identity.RealmServiceIdentityConfiguration;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@QuarkusTest
@TestProfile(DefaultServiceIdentityProviderTest.Profile.class)
public class DefaultServiceIdentityProviderTest {
  private static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  private static final String MY_REALM_KEY = "my-realm";

  @InjectMock RealmContext realmContext;
  @Inject ServiceIdentityConfiguration serviceIdentityConfiguration;

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
  void testRealmServiceIdentityConfigToServiceIdentityCredential() {
    // Check the default realm
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(DEFAULT_REALM_KEY);
    DefaultServiceIdentityProvider defaultProvider =
        new DefaultServiceIdentityProvider(realmContext, serviceIdentityConfiguration);
    EnumMap<ServiceIdentityType, ServiceIdentityCredential> identityCredentials =
        defaultProvider.getServiceIdentityCredentials();

    Assertions.assertThat(identityCredentials)
        .containsKey(ServiceIdentityType.AWS_IAM)
        .size()
        .isEqualTo(1);
    AwsIamServiceIdentityCredential awsIamCredential =
        (AwsIamServiceIdentityCredential) identityCredentials.get(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(awsIamCredential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(awsIamCredential.getIdentityInfoReference())
        .isEqualTo(
            new ServiceSecretReference(
                "urn:polaris-secret:default-identity-provider:system:default:AWS_IAM", Map.of()));
    Assertions.assertThat(
            awsIamCredential.getAwsCredentialsProvider() instanceof DefaultCredentialsProvider)
        .isTrue();

    // Check the my-realm
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn(MY_REALM_KEY);
    DefaultServiceIdentityProvider myRealmProvider =
        new DefaultServiceIdentityProvider(realmContext, serviceIdentityConfiguration);
    identityCredentials = myRealmProvider.getServiceIdentityCredentials();

    Assertions.assertThat(identityCredentials)
        .containsKey(ServiceIdentityType.AWS_IAM)
        .size()
        .isEqualTo(1);
    awsIamCredential =
        (AwsIamServiceIdentityCredential) identityCredentials.get(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(awsIamCredential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");
    Assertions.assertThat(awsIamCredential.getIdentityInfoReference())
        .isEqualTo(
            new ServiceSecretReference(
                "urn:polaris-secret:default-identity-provider:my-realm:AWS_IAM", Map.of()));
    Assertions.assertThat(
            awsIamCredential.getAwsCredentialsProvider() instanceof StaticCredentialsProvider)
        .isTrue();
    StaticCredentialsProvider staticCredentialsProvider =
        (StaticCredentialsProvider) awsIamCredential.getAwsCredentialsProvider();
    Assertions.assertThat(
            staticCredentialsProvider.resolveCredentials() instanceof AwsSessionCredentials)
        .isTrue();
    AwsSessionCredentials awsSessionCredentials =
        (AwsSessionCredentials) staticCredentialsProvider.resolveCredentials();
    Assertions.assertThat(awsSessionCredentials.accessKeyId()).isEqualTo("access-key-id");
    Assertions.assertThat(awsSessionCredentials.secretAccessKey()).isEqualTo("secret-access-key");
    Assertions.assertThat(awsSessionCredentials.sessionToken()).isEqualTo("session-token");

    // Check the other realm which does not exist in the configuration, should fallback to default
    Mockito.when(realmContext.getRealmIdentifier()).thenReturn("other-realm");
    DefaultServiceIdentityProvider otherProvider =
        new DefaultServiceIdentityProvider(realmContext, serviceIdentityConfiguration);
    identityCredentials = otherProvider.getServiceIdentityCredentials();
    Assertions.assertThat(identityCredentials)
        .containsKey(ServiceIdentityType.AWS_IAM)
        .size()
        .isEqualTo(1);
    awsIamCredential =
        (AwsIamServiceIdentityCredential) identityCredentials.get(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(awsIamCredential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(awsIamCredential.getIdentityInfoReference())
        .isEqualTo(
            new ServiceSecretReference(
                "urn:polaris-secret:default-identity-provider:system:default:AWS_IAM", Map.of()));
    Assertions.assertThat(
            awsIamCredential.getAwsCredentialsProvider() instanceof DefaultCredentialsProvider)
        .isTrue();
  }
}
