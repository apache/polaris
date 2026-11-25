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

package org.apache.polaris.core.storage;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(SoftAssertionsExtension.class)
public class PolarisStorageConfigurationInfoTest {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void reserialization(PolarisStorageConfigurationInfo configInfo, String serialized)
      throws Exception {
    var jsonStr = configInfo.serialize();

    var asJsonNode = JsonMapper.shared().readValue(jsonStr, JsonNode.class);
    var serializedJsonNode = JsonMapper.shared().readValue(serialized, JsonNode.class);
    soft.assertThat(asJsonNode).isEqualTo(serializedJsonNode);

    var deserialized =
        JsonMapper.shared().readValue(serialized, PolarisStorageConfigurationInfo.class);
    soft.assertThat(deserialized).isEqualTo(configInfo);

    var reserialized =
        JsonMapper.shared().readValue(jsonStr, PolarisStorageConfigurationInfo.class);
    soft.assertThat(reserialized).isEqualTo(configInfo);
  }

  static Stream<Arguments> reserialization() {
    return Stream.of(
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .region("no-where-1")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"region\":\"no-where-1\",\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .region("no-where-1")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .externalId("external-id")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"externalId\":\"external-id\",\"region\":\"no-where-1\",\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .region("no-where-1")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .externalId("external-id")
                .endpoint("http://127.9.9.9/")
                .stsEndpoint("http://127.9.9.9/sts/")
                .endpointInternal("http://127.8.8.8/internal/")
                .pathStyleAccess(true)
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"externalId\":\"external-id\",\"region\":\"no-where-1\",\"endpoint\":\"http://127.9.9.9/\",\"stsEndpoint\":\"http://127.9.9.9/sts/\",\"endpointInternal\":\"http://127.8.8.8/internal/\",\"pathStyleAccess\":true,\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .region("no-where-1")
                .storageName("my-storage")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"storageName\":\"my-storage\",\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"region\":\"no-where-1\",\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://other-bucket/base/")
                .credentialVendingMechanism("DOWNSTREAM_MECHANISM")
                .endpoint("https://s3.example-compatible-store.test")
                .pathStyleAccess(true)
                .region("auto")
                .storageName("other")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"credentialVendingMechanism\":\"DOWNSTREAM_MECHANISM\",\"allowedLocations\":[\"s3://other-bucket/base/\"],\"storageName\":\"other\",\"region\":\"auto\",\"endpoint\":\"https://s3.example-compatible-store.test\",\"pathStyleAccess\":true,\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        //
        arguments(
            GcpStorageConfigurationInfo.builder()
                .addAllowedLocations("gs://foo/bar", "gs://meep/moo")
                .build(),
            "{\"@type\":\"GcpStorageConfigurationInfo\",\"allowedLocations\":[\"gs://foo/bar\",\"gs://meep/moo\"],\"storageType\":\"GCS\",\"fileIoImplClassName\":\"org.apache.iceberg.gcp.gcs.GCSFileIO\"}"),
        //
        arguments(
            AzureStorageConfigurationInfo.builder()
                .addAllowedLocations("abfs://foo@bar.baz/", "abfss://boo@meep.buzz/")
                .tenantId("tenant-id")
                .build(),
            "{\"@type\":\"AzureStorageConfigurationInfo\",\"allowedLocations\":[\"abfs://foo@bar.baz/\",\"abfss://boo@meep.buzz/\"],\"tenantId\":\"tenant-id\",\"storageType\":\"AZURE\",\"fileIoImplClassName\":\"org.apache.iceberg.azure.adlsv2.ADLSFileIO\"}"),
        //
        arguments(
            FileStorageConfigurationInfo.builder()
                .addAllowedLocations("file:///tmp/bar", "file:///meep/moo")
                .build(),
            "{\"@type\":\"FileStorageConfigurationInfo\",\"allowedLocations\":[\"file:///tmp/bar\",\"file:///meep/moo\"],\"storageType\":\"FILE\",\"fileIoImplClassName\":\"org.apache.iceberg.hadoop.HadoopFileIO\"}"));
  }

  @ParameterizedTest
  @MethodSource
  public void forEntityPathStructuredLocations(
      List<PolarisEntity> entityPath, String location, boolean allowed) {
    RealmConfig realmConfig = new RealmConfigImpl(EMPTY_CONFIG, () -> "test-realm");
    TableIdentifier identifier = TableIdentifier.of("ns", "t");
    LocationRestrictions restrictions =
        PolarisStorageConfigurationInfo.forEntityPath(realmConfig, entityPath).orElseThrow();
    if (allowed) {
      soft.assertThatCode(() -> restrictions.validate(realmConfig, identifier, Set.of(location)))
          .doesNotThrowAnyException();
    } else {
      soft.assertThatThrownBy(
              () -> restrictions.validate(realmConfig, identifier, Set.of(location)))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Invalid locations");
    }
  }

  static Stream<Arguments> forEntityPathStructuredLocations() {
    PolarisEntity catalog =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.CATALOG)
            .setName("catalog")
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                    AwsStorageConfigurationInfo.builder()
                        .addAllowedLocations("s3://lake/")
                        .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                        .build()
                        .serialize()))
            .build();
    PolarisEntity namespace = entity(PolarisEntityType.NAMESPACE, "ns", "s3://lake/ns/");
    PolarisEntity table = entity(PolarisEntityType.TABLE_LIKE, "t", "s3://lake/ns/t/");
    // A table outside its namespace, as left behind by renaming it from another namespace.
    PolarisEntity movedTable =
        entity(PolarisEntityType.TABLE_LIKE, "moved", "s3://lake/other/moved/");
    // Tables created by notifications have no base location.
    PolarisEntity tableWithoutLocation = entity(PolarisEntityType.TABLE_LIKE, "notified", null);

    List<PolarisEntity> namespacePath = List.of(catalog, namespace);
    List<PolarisEntity> tablePath = List.of(catalog, namespace, table);
    List<PolarisEntity> movedTablePath = List.of(catalog, namespace, movedTable);
    List<PolarisEntity> noLocationTablePath = List.of(catalog, namespace, tableWithoutLocation);
    return Stream.of(
        arguments(namespacePath, "s3://lake/ns/t/", true),
        arguments(namespacePath, "s3://lake/other/t/", false),
        arguments(tablePath, "s3://lake/ns/t/", true),
        arguments(tablePath, "s3://lake/ns/t/data/", true),
        arguments(tablePath, "s3://lake/ns/elsewhere/", false),
        arguments(movedTablePath, "s3://lake/other/moved/", false),
        arguments(noLocationTablePath, "s3://lake/ns/notified/", true),
        arguments(noLocationTablePath, "s3://lake/other/notified/", false));
  }

  private static PolarisEntity entity(PolarisEntityType type, String name, String location) {
    PolarisEntity.Builder builder = new PolarisEntity.Builder().setType(type).setName(name);
    if (type == PolarisEntityType.TABLE_LIKE) {
      builder.setSubType(PolarisEntitySubType.ICEBERG_TABLE);
    }
    if (location != null) {
      builder.setProperties(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, location));
    }
    return builder.build();
  }
}
