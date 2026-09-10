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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import jakarta.ws.rs.core.Response;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.exception.IcebergJsonProcessingExceptionMapper;
import org.junit.jupiter.api.Test;

/** The management API's JSON handling of {@code credentialIssuer}: absent, null and unknown. */
class StorageConfigInfoJsonTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void absentAndNullIssuerDeserializeAsNull() throws Exception {
    StorageConfigInfo absent =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"]}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) absent).getCredentialIssuer()).isNull();
    StorageConfigInfo explicitNull =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],\"credentialIssuer\":null}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) explicitNull).getCredentialIssuer()).isNull();
  }

  @Test
  void knownIssuerDeserializes() throws Exception {
    StorageConfigInfo r2 =
        mapper.readValue(
            "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],\"credentialIssuer\":\"CLOUDFLARE_R2\"}",
            StorageConfigInfo.class);
    assertThat(((AwsStorageConfigInfo) r2).getCredentialIssuer())
        .isEqualTo(AwsStorageConfigInfo.CredentialIssuerEnum.CLOUDFLARE_R2);
  }

  @Test
  void unknownIssuerIsA400NeverA500() {
    String body =
        "{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://b/p/\"],\"credentialIssuer\":\"BOGUS\"}";
    assertThatThrownBy(() -> mapper.readValue(body, StorageConfigInfo.class))
        .isInstanceOf(InvalidFormatException.class)
        .hasMessageContaining("BOGUS");
    JsonProcessingException failure = null;
    try {
      mapper.readValue(body, StorageConfigInfo.class);
    } catch (JsonProcessingException e) {
      failure = e;
    }
    try (Response response = new IcebergJsonProcessingExceptionMapper().toResponse(failure)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
  }
}
