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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * In a PR-A build every Iceberg route that opens a CLOUDFLARE_R2 catalog is refused at
 * initialization, namespace reads included, with or without SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION;
 * an STS catalog in the same realm is untouched. The R2 catalog is produced by updating an STS
 * catalog under ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES, because nothing can be created
 * inside an R2 catalog through the REST API in this build. Every catalog gets its own allowed
 * location: upstream rejects overlapping catalog locations at create and update.
 */
class S3CredentialIssuerRoutesTest {

  private static final String R2_ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";
  private static final String NOT_AVAILABLE =
      "S3 credential issuer CLOUDFLARE_R2 is not available in this build";

  /** A mutable config map: TestServices reads it live, so a test can flip the realm allowlist. */
  private static Map<String, Object> config(boolean skipSubscoping) {
    Map<String, Object> config = new HashMap<>();
    config.put("SUPPORTED_S3_CREDENTIAL_ISSUERS", List.of("STS", "CLOUDFLARE_R2"));
    config.put("ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES", true);
    config.put("SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION", skipSubscoping);
    return config;
  }

  private static TestServices services(Map<String, Object> config) {
    return TestServices.builder().config(config).build();
  }

  private static Catalog stsCatalog(String name) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties("s3://bucket/base/" + name))
        .setStorageConfigInfo(
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setRoleArn("arn:aws:iam::123456789012:role/r")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build())
        .build();
  }

  private static void createCatalog(TestServices svc, String name) {
    try (Response r =
        svc.catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(stsCatalog(name)),
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private static void createNamespace(TestServices svc, String catalog, String ns) {
    try (Response r =
        svc.restApi()
            .createNamespace(
                catalog,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(ns)).build(),
                null,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static void switchToCloudflareR2(TestServices svc, String name) {
    Catalog fetched;
    try (Response r =
        svc.catalogsApi().getCatalog(name, svc.realmContext(), svc.securityContext())) {
      fetched = (Catalog) r.getEntity();
    }
    UpdateCatalogRequest toR2 =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/base/" + name),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialIssuer(AwsStorageConfigInfo.CredentialIssuerEnum.CLOUDFLARE_R2)
                .setEndpoint(R2_ENDPOINT)
                .setPathStyleAccess(true)
                .setRegion("auto")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build());
    try (Response r =
        svc.catalogsApi().updateCatalog(name, toR2, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void everyIcebergRouteOnACloudflareR2CatalogIsRefusedAtInitialization(boolean skipSubscoping) {
    TestServices svc = services(config(skipSubscoping));
    createCatalog(svc, "r2cat");
    createNamespace(svc, "r2cat", "ns");
    createCatalog(svc, "stscat");
    switchToCloudflareR2(svc, "r2cat");

    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "r2cat", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .loadNamespaceMetadata(
                        "r2cat", "ns", svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .createNamespace(
                        "r2cat",
                        CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns2")).build(),
                        null,
                        svc.realmContext(),
                        svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);

    // Management reads are unaffected, and the STS catalog in the same realm still serves.
    try (Response r =
        svc.catalogsApi().getCatalog("r2cat", svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response r =
        svc.restApi()
            .listNamespaces(
                "stscat", null, null, null, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theRealmKillSwitchRefusesTheR2CatalogWithNotEnabled(boolean skipSubscoping) {
    Map<String, Object> config = config(skipSubscoping);
    TestServices svc = services(config);
    createCatalog(svc, "r2kill");
    createNamespace(svc, "r2kill", "ns");
    createCatalog(svc, "stskill");
    switchToCloudflareR2(svc, "r2kill");

    // Engage the kill switch: the realm no longer lists CLOUDFLARE_R2.
    config.put("SUPPORTED_S3_CREDENTIAL_ISSUERS", List.of("STS"));

    String notEnabled = "S3 credential issuer CLOUDFLARE_R2 is not enabled in this realm";
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "r2kill", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .loadNamespaceMetadata(
                        "r2kill", "ns", svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    // Management reads still work; an update carrying a storage config is refused at site 1.
    Catalog fetched;
    try (Response r =
        svc.catalogsApi().getCatalog("r2kill", svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      fetched = (Catalog) r.getEntity();
    }
    UpdateCatalogRequest touch =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/base/r2kill"),
            fetched.getStorageConfigInfo());
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .updateCatalog("r2kill", touch, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    // The STS catalog in the same realm is unaffected.
    try (Response r =
        svc.restApi()
            .listNamespaces(
                "stskill", null, null, null, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }
}
