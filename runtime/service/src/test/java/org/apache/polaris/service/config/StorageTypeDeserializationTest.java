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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for case-insensitive storage type deserialization. */
public class StorageTypeDeserializationTest {

  private ObjectMapper mapper;

  @BeforeEach
  public void setup() {
    mapper = JsonMapper.builder().build();
    Serializers.registerSerializers(mapper);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "S3"})
  void testS3StorageTypeCaseInsensitive(String storageType) throws Exception {
    String json =
        String.format(
            "{\"storageType\":\"%s\",\"roleArn\":\"arn:aws:iam::123456789012:role/test\",\"allowedLocations\":[\"s3://bucket/\"]}",
            storageType);
    StorageConfigInfo config = mapper.readValue(json, StorageConfigInfo.class);
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
    assertThat(config).isInstanceOf(AwsStorageConfigInfo.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gcs", "GCS", "Gcs", "gCs"})
  void testGcsStorageTypeCaseInsensitive(String storageType) throws Exception {
    String json =
        String.format(
            "{\"storageType\":\"%s\",\"allowedLocations\":[\"gs://bucket/\"]}",
            storageType);
    StorageConfigInfo config = mapper.readValue(json, StorageConfigInfo.class);
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.GCS);
    assertThat(config).isInstanceOf(GcpStorageConfigInfo.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"azure", "AZURE", "Azure", "aZuRe"})
  void testAzureStorageTypeCaseInsensitive(String storageType) throws Exception {
    String json =
        String.format(
            "{\"storageType\":\"%s\",\"tenantId\":\"tenant-id\",\"allowedLocations\":[\"abfss://container@account.dfs.core.windows.net/\"]}",
            storageType);
    StorageConfigInfo config = mapper.readValue(json, StorageConfigInfo.class);
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.AZURE);
    assertThat(config).isInstanceOf(AzureStorageConfigInfo.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"file", "FILE", "File", "FiLe"})
  void testFileStorageTypeCaseInsensitive(String storageType) throws Exception {
    String json =
        String.format(
            "{\"storageType\":\"%s\",\"allowedLocations\":[\"file:///tmp/\"]}",
            storageType);
    StorageConfigInfo config = mapper.readValue(json, StorageConfigInfo.class);
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.FILE);
    assertThat(config).isInstanceOf(FileStorageConfigInfo.class);
    
  }

  @Test
  void testInvalidStorageType() {
    String json = "{\"storageType\":\"invalid\",\"allowedLocations\":[]}";
    assertThatThrownBy(() -> mapper.readValue(json, StorageConfigInfo.class))
        .isInstanceOf(JsonMappingException.class)
        .hasMessageContaining("Invalid storage type 'invalid'")
        .hasMessageContaining("Valid values are: S3, GCS, AZURE, FILE");
  }

  @Test
  void testNullStorageType() throws Exception {
    String json = "{\"storageType\":null,\"allowedLocations\":[]}";
    // Null storage type should fail validation during object construction
    assertThatThrownBy(() -> mapper.readValue(json, StorageConfigInfo.class))
        .isInstanceOf(Exception.class); // May be validation exception or mapping exception
  }
}
