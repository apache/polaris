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

package org.apache.polaris.service.quarkus.identity;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.registry.DefaultServiceIdentityRegistry;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistryFactory;
import org.apache.polaris.core.identity.resolved.ResolvedAwsIamServiceIdentity;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DefaultServiceIdentityRegistryTest.Profile.class)
public class DefaultServiceIdentityRegistryTest {
  private static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  private static final String MY_REALM_KEY = "my-realm";

  @Inject QuarkusServiceIdentityConfiguration serviceIdentityConfiguration;
  @Inject ServiceIdentityRegistryFactory serviceIdentityRegistryFactory;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.identity-registry.type",
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
    QuarkusRealmServiceIdentityConfiguration defaultConfig =
        serviceIdentityConfiguration.forRealm(DEFAULT_REALM_KEY);
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().isPresent()).isTrue();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().accessKeyId()).isEmpty();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().secretAccessKey()).isEmpty();
    Assertions.assertThat(defaultConfig.awsIamServiceIdentity().get().sessionToken()).isEmpty();

    // Check the my-realm configuration
    QuarkusRealmServiceIdentityConfiguration myRealmConfig =
        serviceIdentityConfiguration.forRealm(MY_REALM_KEY);
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
    QuarkusRealmServiceIdentityConfiguration otherConfig =
        serviceIdentityConfiguration.forRealm("other-realm");
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().isPresent()).isTrue();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().iamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().accessKeyId()).isEmpty();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().secretAccessKey()).isEmpty();
    Assertions.assertThat(otherConfig.awsIamServiceIdentity().get().sessionToken()).isEmpty();
  }

  @Test
  void testRealmServiceIdentityConfigToResolvedServiceIdentity() {
    // Check the default realm
    DefaultServiceIdentityRegistry defaultRegistry =
        (DefaultServiceIdentityRegistry)
            serviceIdentityRegistryFactory.getOrCreateServiceIdentityRegistry(
                () -> DEFAULT_REALM_KEY);
    EnumMap<ServiceIdentityType, ResolvedServiceIdentity> resolvedIdentities =
        defaultRegistry.getResolvedServiceIdentities();

    Assertions.assertThat(resolvedIdentities)
        .containsKey(ServiceIdentityType.AWS_IAM)
        .size()
        .isEqualTo(1);
    ResolvedAwsIamServiceIdentity resolvedAwsIamServiceIdentity =
        (ResolvedAwsIamServiceIdentity) resolvedIdentities.get(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-default-iam-user");
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getIdentityInfoReference())
        .isEqualTo(
            new ServiceSecretReference(
                "urn:polaris-service-secret:default-identity-registry:system:default:AWS_IAM",
                Map.of()));
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getAccessKeyId()).isNull();
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getSecretAccessKey()).isNull();
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getSessionToken()).isNull();

    // Check the my-realm
    DefaultServiceIdentityRegistry myRealmRegistry =
        (DefaultServiceIdentityRegistry)
            serviceIdentityRegistryFactory.getOrCreateServiceIdentityRegistry(() -> MY_REALM_KEY);
    resolvedIdentities = myRealmRegistry.getResolvedServiceIdentities();

    Assertions.assertThat(resolvedIdentities)
        .containsKey(ServiceIdentityType.AWS_IAM)
        .size()
        .isEqualTo(1);
    resolvedAwsIamServiceIdentity =
        (ResolvedAwsIamServiceIdentity) resolvedIdentities.get(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getIdentityInfoReference())
        .isEqualTo(
            new ServiceSecretReference(
                "urn:polaris-service-secret:default-identity-registry:my-realm:AWS_IAM", Map.of()));
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getAccessKeyId())
        .isEqualTo("access-key-id");
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getSecretAccessKey())
        .isEqualTo("secret-access-key");
    Assertions.assertThat(resolvedAwsIamServiceIdentity.getSessionToken())
        .isEqualTo("session-token");

    // Check the other realm
    DefaultServiceIdentityRegistry otherRegistry =
        (DefaultServiceIdentityRegistry)
            serviceIdentityRegistryFactory.getOrCreateServiceIdentityRegistry(() -> "other-realm");
    Assertions.assertThat(otherRegistry).isEqualTo(defaultRegistry);
  }
}
