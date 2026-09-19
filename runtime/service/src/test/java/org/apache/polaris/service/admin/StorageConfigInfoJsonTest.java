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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;

/**
 * The management API's JSON handling of {@code credentialVendingMechanism}: absent, null and
 * unknown.
 */
class StorageConfigInfoJsonTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void absentAndNullMechanismDeserializeAsNull() throws Exception {
    StorageConfigInfo absent =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"]}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) absent).getCredentialVendingMechanism()).isNull();
    StorageConfigInfo explicitNull =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],"
                + "\"credentialVendingMechanism\":null}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) explicitNull).getCredentialVendingMechanism()).isNull();
  }

  @Test
  void knownMechanismDeserializes() throws Exception {
    StorageConfigInfo sts =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],"
                + "\"credentialVendingMechanism\":\"STS\"}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) sts).getCredentialVendingMechanism()).isEqualTo("STS");
  }

  /**
   * An unknown mechanism name is not a Jackson type error: the string field deserializes as is, and
   * the realm allowlist refuses it at catalog create.
   */
  @Test
  void unknownMechanismDeserializesAndIsRefusedByTheAllowlist() throws Exception {
    String body =
        "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],"
            + "\"credentialVendingMechanism\":\"BOGUS\"}";
    StorageConfigInfo deserialized = mapper.readValue(body, StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) deserialized).getCredentialVendingMechanism())
        .isEqualTo("BOGUS");

    TestServices svc =
        TestServices.builder()
            .config(Map.of("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3")))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("bogus-mechanism")
            .setProperties(new CatalogProperties("s3://bucket/base/"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn("arn:aws:iam::123456789012:role/r")
                    .setCredentialVendingMechanism("BOGUS")
                    .setAllowedLocations(List.of("s3://bucket/base/"))
                    .build())
            .build();
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .createCatalog(
                        new CreateCatalogRequest(catalog),
                        svc.realmContext(),
                        svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism BOGUS is not enabled in this realm");
  }
}
