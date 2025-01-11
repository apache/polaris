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
package org.apache.polaris.core.storage.cache;

import static org.apache.polaris.core.storage.PolarisCredentialVendor.*;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.polaris.core.persistence.*;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StorageCredentialCacheEntryTest {

  public StorageCredentialCacheEntryTest() {}

  @Test
  public void testBadResult() {
    ScopedCredentialsResult badResult =
        new ScopedCredentialsResult(
            BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, "extra_error_info");
    StorageCredentialCacheEntry cacheEntry = new StorageCredentialCacheEntry(badResult);
    Assertions.assertThatThrownBy(() -> cacheEntry.convertToMapOfString())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("because \"this.credsMap\" is null");
  }

  @ParameterizedTest
  @MethodSource("testConvertToMapOfStringParams")
  public void testConvertToMapOfString(
      Map<PolarisCredentialProperty, String> original, Map<String, String> expected) {
    EnumMap<PolarisCredentialProperty, String> props =
        new EnumMap<>(PolarisCredentialProperty.class);
    props.putAll(original);

    ScopedCredentialsResult goodResult = new ScopedCredentialsResult(props);
    StorageCredentialCacheEntry cacheEntry = new StorageCredentialCacheEntry(goodResult);
    Map<String, String> properties = cacheEntry.convertToMapOfString();
    Assertions.assertThat(properties).isEqualTo(expected);
  }

  private static Stream<Arguments> testConvertToMapOfStringParams() {
    return Stream.of(
        Arguments.of(
            Map.of(
                PolarisCredentialProperty.AWS_KEY_ID,
                "aws_key_id",
                PolarisCredentialProperty.AWS_SECRET_KEY,
                "aws_secret_access_key",
                PolarisCredentialProperty.AWS_TOKEN,
                "aws_session_token"),
            Map.of(
                S3FileIOProperties.ACCESS_KEY_ID,
                "aws_key_id",
                S3FileIOProperties.SECRET_ACCESS_KEY,
                "aws_secret_access_key",
                S3FileIOProperties.SESSION_TOKEN,
                "aws_session_token")),
        Arguments.of(
            Map.of(
                PolarisCredentialProperty.GCS_ACCESS_TOKEN,
                "gcs_oauth2_token",
                PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT,
                "gcs_token_expires_at"),
            Map.of(
                GCPProperties.GCS_OAUTH2_TOKEN,
                "gcs_oauth2_token",
                GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                "gcs_token_expires_at")),
        Arguments.of(
            Map.of(
                PolarisCredentialProperty.AZURE_ACCOUNT_HOST,
                "azure_storage_account",
                PolarisCredentialProperty.AZURE_SAS_TOKEN,
                "azure_sas_token"),
            Map.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "azure_storage_account",
                "azure_sas_token")),
        Arguments.of(
            Map.of(PolarisCredentialProperty.AZURE_ACCESS_TOKEN, "azure_access_token"),
            Map.of("", "azure_access_token")),
        Arguments.of(
            Map.of(PolarisCredentialProperty.EXPIRATION_TIME, "expiration_time"),
            Map.of("expiration-time", "expiration_time")),
        Arguments.of(
            Map.of(
                PolarisCredentialProperty.ADDITIONAL_STORAGE_CONFIG,
                "{\"s3.session-token-expires-at-ms\": \"expires_at\"}"),
            Map.of(
                "additional-storage-config",
                "{\"s3.session-token-expires-at-ms\": \"expires_at\"}")),
        Arguments.of(Map.of(), Map.of()));
  }
}
