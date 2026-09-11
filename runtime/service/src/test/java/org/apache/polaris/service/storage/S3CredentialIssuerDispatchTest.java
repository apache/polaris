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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

/** Spec 5.3 item 4: the provider's {@code case S3} checks the issuer, then dispatches on it. */
class S3CredentialIssuerDispatchTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String R2_ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";

  private static RealmConfig realmConfig(List<String> issuers) {
    Map<String, Object> config = Map.of("SUPPORTED_S3_CREDENTIAL_ISSUERS", issuers);
    return new RealmConfigImpl((rc, name) -> config.get(name), REALM);
  }

  private static PolarisStorageIntegrationProviderImpl provider(RealmConfig realmConfig) {
    return new PolarisStorageIntegrationProviderImpl(
        destination -> Mockito.mock(StsClient.class),
        Optional.empty(),
        () -> GoogleCredentials.create(new AccessToken("abc", new Date())),
        null,
        realmConfig,
        new PolarisDefaultDiagServiceImpl());
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
        .setCredentialIssuer(AwsStorageConfigInfo.CredentialIssuerEnum.CLOUDFLARE_R2)
        .setEndpoint(R2_ENDPOINT)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  @Test
  void stsAndAbsentIssuerDispatchToTheAwsIntegration() {
    RealmConfig rc = realmConfig(List.of("STS"));
    assertThat(provider(rc).getStorageIntegration(List.of(catalog(rc, sts()))))
        .isInstanceOf(AwsCredentialsStorageIntegration.class);
    AwsStorageConfigInfo explicitSts =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialIssuer(AwsStorageConfigInfo.CredentialIssuerEnum.STS)
            .setRoleArn("arn:aws:iam::123456789012:role/r")
            .setAllowedLocations(List.of("s3://bucket/base/"))
            .build();
    assertThat(provider(rc).getStorageIntegration(List.of(catalog(rc, explicitSts))))
        .isInstanceOf(AwsCredentialsStorageIntegration.class);
  }

  @Test
  void disallowedIssuerIsRejectedBeforeDispatch() {
    RealmConfig rc = realmConfig(List.of("CLOUDFLARE_R2"));
    assertThatThrownBy(() -> provider(rc).getStorageIntegration(List.of(catalog(rc, sts()))))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer STS is not enabled in this realm");
  }

  @Test
  void allowedCloudflareR2HitsTheThrowingArm() {
    RealmConfig rc = realmConfig(List.of("STS", "CLOUDFLARE_R2"));
    assertThatThrownBy(() -> provider(rc).getStorageIntegration(List.of(catalog(rc, r2()))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not available in this build");
  }
}
