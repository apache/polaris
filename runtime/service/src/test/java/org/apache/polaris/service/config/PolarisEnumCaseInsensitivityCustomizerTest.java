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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PolarisEnumCaseInsensitivityCustomizerTest {

  private static ObjectMapper mapper() {
    ObjectMapper mapper = new ObjectMapper();
    new PolarisEnumCaseInsensitivityCustomizer().customize(mapper);
    return mapper;
  }

  static Stream<Arguments> storageTypes() {
    return Stream.of(
        Arguments.of(
            "{\"storageType\":\"s3\",\"roleArn\":\"arn:aws:iam::1:role/x\"}",
            AwsStorageConfigInfo.class),
        Arguments.of(
            "{\"storageType\":\"S3\",\"roleArn\":\"arn:aws:iam::1:role/x\"}",
            AwsStorageConfigInfo.class),
        Arguments.of("{\"storageType\":\"gcs\"}", GcpStorageConfigInfo.class),
        Arguments.of(
            "{\"storageType\":\"Azure\",\"tenantId\":\"t\"}", AzureStorageConfigInfo.class),
        Arguments.of("{\"storageType\":\"file\"}", FileStorageConfigInfo.class));
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void storageTypeDiscriminatorIsCaseInsensitive(
      String json, Class<? extends StorageConfigInfo> expected) throws Exception {
    // the discriminator is both a polymorphic type id and a visible enum, so this
    // exercises ACCEPT_CASE_INSENSITIVE_VALUES (subtype dispatch) and
    // ACCEPT_CASE_INSENSITIVE_ENUMS (the enum property) together
    StorageConfigInfo config = mapper().readValue(json, StorageConfigInfo.class);
    assertThat(config).isInstanceOf(expected);
  }

  @Test
  void lowercaseDeserializesToCanonicalEnumAndSerializesBackCanonical() throws Exception {
    StorageConfigInfo config =
        mapper()
            .readValue(
                "{\"storageType\":\"s3\",\"roleArn\":\"arn:aws:iam::1:role/x\"}",
                StorageConfigInfo.class);
    assertThat(config.getStorageType()).isEqualTo(StorageConfigInfo.StorageTypeEnum.S3);
    // input leniency must not change output: serialization stays canonical
    assertThat(mapper().writeValueAsString(config)).contains("\"storageType\":\"S3\"");
  }

  @Test
  void appliesToPlainEnumsToo() throws Exception {
    Catalog catalog =
        mapper()
            .readValue(
                "{\"type\":\"internal\",\"name\":\"c\","
                    + "\"properties\":{\"default-base-location\":\"file:///tmp/c\"}}",
                Catalog.class);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
  }

  @Test
  void unknownValuesAreStillRejected() {
    ObjectMapper mapper = mapper();
    assertThatThrownBy(() -> mapper.readValue("{\"storageType\":\"S4\"}", StorageConfigInfo.class))
        .isInstanceOf(Exception.class);
  }
}
