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
package org.apache.polaris.service.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

/** The provider's {@code case S3} checks the allowlist, then dispatches through the registry. */
class S3CredentialVendingMechanismDispatchTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String R2_ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";

  private static RealmConfig realmConfig(List<String> mechanisms) {
    Map<String, Object> config = Map.of("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", mechanisms);
    return new RealmConfigImpl((rc, name) -> config.get(name), REALM);
  }

  private static S3CredentialVendingMechanism testStsMechanism() {
    return new StsCredentialVendingMechanism(
        destination -> Mockito.mock(StsClient.class), Optional.empty(), null);
  }

  private static PolarisStorageIntegrationProviderImpl provider(
      RealmConfig realmConfig, Map<String, S3CredentialVendingMechanism> mechanisms) {
    return new PolarisStorageIntegrationProviderImpl(
        new S3CredentialVendingMechanisms(mechanisms),
        () -> GoogleCredentials.create(new AccessToken("abc", new Date())),
        null,
        realmConfig,
        new PolarisDefaultDiagServiceImpl());
  }

  private static PolarisStorageIntegrationProviderImpl provider(RealmConfig realmConfig) {
    return provider(realmConfig, Map.of("STS", testStsMechanism()));
  }

  private static CatalogEntity catalog(RealmConfig realmConfig, AwsStorageConfigInfo model) {
    return new CatalogEntity.Builder()
        .setName("c")
        .addProperty(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, "s3://bucket/base/")
        .setStorageConfigurationInfo(realmConfig, model)
        .build();
  }

  private static AwsStorageConfigInfo sts() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setRoleArn("arn:aws:iam::123456789012:role/r")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  private static AwsStorageConfigInfo r2() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(S3CredentialVendingMechanism.CLOUDFLARE_R2)
        .setEndpoint(R2_ENDPOINT)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  private static AwsStorageConfigInfo withMechanism(String mechanism) {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(mechanism)
        .setRoleArn("arn:aws:iam::123456789012:role/r")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  @Test
  void stsAndAbsentMechanismDispatchToTheAwsIntegration() {
    RealmConfig rc = realmConfig(List.of("STS"));
    PolarisStorageIntegrationProviderImpl provider = provider(rc);
    assertThat(provider.getStorageIntegration(List.of(catalog(rc, sts()))))
        .isInstanceOf(AwsCredentialsStorageIntegration.class);
    assertThat(provider.getStorageIntegration(List.of(catalog(rc, withMechanism("STS")))))
        .isInstanceOf(AwsCredentialsStorageIntegration.class);
  }

  @Test
  void disallowedMechanismIsRejectedBeforeDispatch() {
    RealmConfig rc = realmConfig(List.of("CLOUDFLARE_R2"));
    assertThatThrownBy(() -> provider(rc).getStorageIntegration(List.of(catalog(rc, sts()))))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism STS is not enabled in this realm");
  }

  @Test
  void allowlistedButUninstalledCloudflareR2IsNotAvailable() {
    RealmConfig rc = realmConfig(List.of("STS", "CLOUDFLARE_R2"));
    assertThatThrownBy(() -> provider(rc).getStorageIntegration(List.of(catalog(rc, r2()))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism CLOUDFLARE_R2 is not available in this server");
  }

  @Test
  void anInstalledThirdMechanismIsSelectedWhenAllowlisted() {
    RealmConfig rc = realmConfig(List.of("STS", "TEST_MECHANISM"));
    PolarisStorageIntegration expected = Mockito.mock(PolarisStorageIntegration.class);
    AtomicInteger factoryCalls = new AtomicInteger();
    S3CredentialVendingMechanism testMechanism =
        (storageConfig, callerRealmConfig) -> {
          factoryCalls.incrementAndGet();
          return expected;
        };
    PolarisStorageIntegrationProviderImpl provider =
        provider(rc, Map.of("STS", testStsMechanism(), "TEST_MECHANISM", testMechanism));

    assertThat(
            provider.getStorageIntegration(List.of(catalog(rc, withMechanism("TEST_MECHANISM")))))
        .isSameAs(expected);
    assertThat(factoryCalls).hasValue(1);
  }

  @Test
  void anInstalledThirdMechanismNotAllowlistedIsRefusedBeforeItsFactoryRuns() {
    RealmConfig rc = realmConfig(List.of("STS"));
    AtomicInteger factoryCalls = new AtomicInteger();
    S3CredentialVendingMechanism testMechanism =
        (storageConfig, callerRealmConfig) -> {
          factoryCalls.incrementAndGet();
          return Mockito.mock(PolarisStorageIntegration.class);
        };
    PolarisStorageIntegrationProviderImpl provider =
        provider(rc, Map.of("STS", testStsMechanism(), "TEST_MECHANISM", testMechanism));

    assertThatThrownBy(
            () ->
                provider.getStorageIntegration(
                    List.of(catalog(rc, withMechanism("TEST_MECHANISM")))))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism TEST_MECHANISM is not enabled in this realm");
    assertThat(factoryCalls).hasValue(0);
  }
}
