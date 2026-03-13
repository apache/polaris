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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.storage.RemoteSigningProperty;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.storage.sign.RemoteSigningToken;
import org.apache.polaris.service.storage.sign.RemoteSigningTokenService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the remote signing endpoint authorization logic.
 *
 * <p>Tests for create table, load table and register table with remote signing delegation are in
 * {@link AbstractIcebergCatalogHandlerAuthzTest}.
 */
@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class RemoteSigningAuthzTest {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("ns1"), "tbl1");

  @Inject RealmContextHolder realmContextHolder;
  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;
  @Inject RemoteSigningTokenService remoteSigningTokenService;

  private IcebergCatalogHandler handler;

  @BeforeEach
  void setUp() {
    realmContextHolder.set(() -> "realm1");
    PolarisPrincipal principal = PolarisPrincipal.of("principal1", Map.of(), Set.of());
    handler = icebergCatalogHandlerFactory.createHandler("catalog1", principal);
  }

  @Test
  public void testMissingToken() {
    RemoteSignRequest requestWithInvalidToken =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .build();
    assertThatThrownBy(() -> handler.signRequest(requestWithInvalidToken, TABLE))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Missing remote signing token");
  }

  @Test
  public void testInvalidToken() {
    RemoteSignRequest requestWithInvalidToken =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(Map.of(RemoteSigningProperty.TOKEN.shortName(), "invalid-token"))
            .build();
    assertThatThrownBy(() -> handler.signRequest(requestWithInvalidToken, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid or expired remote signing token");
  }

  @Test
  public void testTokenForDifferentCatalog() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("different-catalog")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different catalog");
  }

  @Test
  public void testTokenForDifferentTable() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TableIdentifier.of(Namespace.of("other"), "table"))
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different table");
  }

  @Test
  public void testTokenForDifferentPrincipal() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("different-principal")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different principal");
  }

  @ParameterizedTest
  @ValueSource(strings = {"PUT", "POST", "DELETE", "PATCH"})
  public void testWriteOperationNotAllowed(String method) {
    // Create a token that only allows read operations
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(false)
            .build();
    // But try to make a write request (PUT)
    RemoteSignRequest writeRequest =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method(method)
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(writeRequest, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Not authorized to sign write request");
  }

  @Test
  public void testInvalidRequestUri() {
    URI invalidUri = URI.create("https://invalid.com");
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest invalidRequest =
        ImmutableRemoteSignRequest.builder()
            .uri(invalidUri)
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.PATH_STYLE_ACCESS.shortName(),
                    "true"))
            .build();
    assertThatThrownBy(() -> handler.signRequest(invalidRequest, TABLE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid S3 URL: https://invalid.com");
  }

  @Test
  public void testInvalidProvider() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("INVALID")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Invalid remote signing storage provider: INVALID");
  }

  @ParameterizedTest
  @CsvSource({"GCS, GCS", "gcs, GCS", "ADLS, AZURE", "adls, AZURE"})
  public void testUnsupportedProvider(String provider, String storageType) {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://example-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider(provider)
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Remote signing is not supported for storage type: " + storageType);
  }

  @Test
  public void testForbiddenLocation() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("principal1")
            .catalogName("catalog1")
            .tableIdentifier(TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://allowed-bucket/")))
            .readWrite(true)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://forbidden-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .provider("S3")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> handler.signRequest(request, TABLE))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining(
            "s3://forbidden-bucket/some-object is not in the list of allowed locations: [s3://allowed-bucket]");
  }
}
