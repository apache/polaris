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

import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BASE;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BUCKET_ROOT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.ENDPOINT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.FIXED_TOKEN;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.OTHER_ENDPOINT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.catalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createCatalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createCatalogResponse;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.fetchCatalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.otherMechanismConfig;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.r2Config;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.services;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.statusOf;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.updateCatalogResponse;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Catalog create and update for the {@code R2_LOCAL_SIGNING} mechanism, through the management
 * resource layer on {@link TestServices}. The mechanism's {@code validate} runs inside {@code
 * PolarisAdminService}, so these tests show the whole path: realm allowlist, registry, then the
 * mechanism's rules.
 */
class R2LocalSigningCatalogFlowTest {

  private static final List<String> ALLOWED = List.of(BASE + "/");
  private static final String FOR_MECHANISM =
      " for the " + R2_LOCAL_SIGNING + " credential vending mechanism";
  private static final String KMS_KEY = "arn:aws:kms:us-east-1:123456789012:key/test-key";

  private final TestServices services = services(Map.of(), FIXED_TOKEN);

  @Test
  void anAllowlistedCatalogIsCreatedAndReadBack() {
    createCatalog(services, catalog("r2-on", r2Config(ALLOWED).build()));

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "r2-on").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
    assertThat(fetched.getEndpoint()).isEqualTo(ENDPOINT);
    assertThat(fetched.getPathStyleAccess()).isTrue();
    assertThat(fetched.getRegion()).isEqualTo("auto");
  }

  @Test
  void aCatalogIsRefusedWith400WhenTheRealmDoesNotAllowlistTheMechanism() {
    TestServices stsOnly =
        services(Map.of("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS")), FIXED_TOKEN);

    Throwable thrown =
        catchThrowable(
            () ->
                createCatalogResponse(stsOnly, catalog("r2-off", r2Config(ALLOWED).build()))
                    .close());

    assertThat(thrown)
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism R2_LOCAL_SIGNING is not enabled in this realm");
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  void aBadEndpointIsRefusedWith400() {
    Throwable thrown =
        catchThrowable(
            () ->
                createCatalogResponse(
                        services,
                        catalog(
                            "r2-bad-endpoint",
                            r2Config(ALLOWED).setEndpoint("https://s3.amazonaws.com").build()))
                    .close());

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a Cloudflare R2 endpoint");
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  /**
   * Core copies {@code roleArn} and {@code decryptionKeys} from the request as they are, and it
   * stores {@code encryptionKeys}, {@code currentKmsKey}, and {@code allowedKmsKeys} as {@code
   * encryptionKeys}. So the refusal of an encryption key names all three request fields.
   */
  static Stream<Arguments> stsAndKmsRequestFields() {
    String encryptionKeysRefusal =
        "encryptionKeys, currentKmsKey, and allowedKmsKeys must not be set" + FOR_MECHANISM;
    return Stream.of(
        Arguments.of(
            "roleArn",
            r2Config(ALLOWED).setRoleArn("arn:aws:iam::123456789012:role/test-role").build(),
            "roleArn must not be set" + FOR_MECHANISM),
        Arguments.of(
            "currentKmsKey",
            r2Config(ALLOWED).setCurrentKmsKey(KMS_KEY).build(),
            encryptionKeysRefusal),
        Arguments.of(
            "allowedKmsKeys",
            r2Config(ALLOWED).setAllowedKmsKeys(List.of(KMS_KEY)).build(),
            encryptionKeysRefusal),
        Arguments.of(
            "encryptionKeys",
            r2Config(ALLOWED).setEncryptionKeys(List.of(KMS_KEY)).build(),
            encryptionKeysRefusal),
        Arguments.of(
            "decryptionKeys",
            r2Config(ALLOWED).setDecryptionKeys(List.of(KMS_KEY)).build(),
            "decryptionKeys must not be set" + FOR_MECHANISM));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stsAndKmsRequestFields")
  void anStsOrKmsRequestFieldIsRefusedWith400(
      String field, AwsStorageConfigInfo storage, String expected) {
    Throwable thrown =
        catchThrowable(
            () -> createCatalogResponse(services, catalog("r2-sts-kms", storage)).close());

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessage(expected);
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  void aCreateWithoutAllowedLocationsIsRefusedWhenTheDefaultBaseLocationHasADoubledSlash() {
    // No allowed list: Polaris uses the default base location as the only allowed location.
    AwsStorageConfigInfo.Builder noAllowedList =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism(R2_LOCAL_SIGNING)
            .setEndpoint(ENDPOINT)
            .setPathStyleAccess(true)
            .setRegion("auto");

    Throwable thrown =
        catchThrowable(
            () ->
                createCatalogResponse(
                        services,
                        catalog(
                            "r2-doubled-default",
                            BUCKET_ROOT + "//r2/",
                            noAllowedList.build(),
                            false))
                    .close());

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("contains an empty path segment ('//')");
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  void aCreateWithAnExplicitAllowedListIsAcceptedWhenOnlyTheDefaultBaseLocationHasADoubledSlash() {
    // The empty-segment rule at create sees only the allowed locations. A table location with an
    // empty path segment is refused at vend time. R2LocalSigningScopeFlowTest pins that.
    createCatalog(
        services,
        catalog(
            "r2-doubled-base",
            BUCKET_ROOT + "//r2/",
            r2Config(List.of(BUCKET_ROOT + "/")).build(),
            false));

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "r2-doubled-base").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
    assertThat(fetched.getAllowedLocations()).containsExactly(BUCKET_ROOT + "/");
  }

  /**
   * The Python CLI sends {@code stsUnavailable=false} and {@code kmsUnavailable=false} by default.
   * Only {@code true} is refused, so a catalog created that way is accepted.
   */
  @Test
  void aCatalogWithTheCliDefaultFlagsIsAccepted() {
    createCatalog(
        services,
        catalog(
            "r2-cli", r2Config(ALLOWED).setStsUnavailable(false).setKmsUnavailable(false).build()));

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "r2-cli").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
  }

  @Test
  void aCatalogUnderAnotherMechanismUpdatesToR2WithTheSameEndpoint() {
    createCatalog(services, catalog("switch", otherMechanismConfig(ALLOWED).build()));

    try (Response response = updateCatalogResponse(services, "switch", r2Config(ALLOWED).build())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "switch").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
    assertThat(fetched.getEndpoint()).isEqualTo(ENDPOINT);
  }

  @Test
  void anUpdateToR2WithANewEndpointIsAccepted() {
    createCatalog(services, catalog("moved", otherMechanismConfig(ALLOWED).build()));

    try (Response response =
        updateCatalogResponse(
            services, "moved", r2Config(ALLOWED).setEndpoint(OTHER_ENDPOINT).build())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "moved").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
    assertThat(fetched.getEndpoint()).isEqualTo(OTHER_ENDPOINT);
  }

  @Test
  void anR2CatalogUpdatesToANewEndpoint() {
    createCatalog(services, catalog("r2-moved", r2Config(ALLOWED).build()));

    try (Response response =
        updateCatalogResponse(
            services, "r2-moved", r2Config(ALLOWED).setEndpoint(OTHER_ENDPOINT).build())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetchCatalog(services, "r2-moved").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(R2_LOCAL_SIGNING);
    assertThat(fetched.getEndpoint()).isEqualTo(OTHER_ENDPOINT);
  }
}
