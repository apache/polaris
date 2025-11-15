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

package org.apache.polaris.service.storage.s3.sign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.aws.s3.signer.ImmutableS3SignRequest;
import org.apache.iceberg.aws.s3.signer.S3SignRequest;
import org.apache.iceberg.aws.s3.signer.S3SignResponse;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@ExtendWith(MockitoExtension.class)
class S3RequestSignerImplTest {

  @Mock private StorageConfiguration storageConfiguration;
  @Mock private AwsCredentialsProvider awsCredentialsProvider;

  @InjectMocks private S3RequestSignerImpl s3RequestSigner;

  private static final String TEST_ACCESS_KEY = "test-access-key";
  private static final String TEST_SECRET_KEY = "test-secret-key";
  private static final String TEST_REGION = "us-west-2";
  private static final String TEST_HOST = "test-bucket.s3.us-west-2.amazonaws.com";
  private static final URI TEST_URI = URI.create("https://" + TEST_HOST + "/test-path");

  @BeforeEach
  void setUp() {
    AwsCredentials credentials = AwsBasicCredentials.create(TEST_ACCESS_KEY, TEST_SECRET_KEY);
    when(storageConfiguration.awsSystemCredentials())
        .thenReturn(Optional.of(awsCredentialsProvider));
    when(awsCredentialsProvider.resolveCredentials()).thenReturn(credentials);
  }

  @Test
  void testGET() {
    // Given
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testPUT() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("PUT")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testPOST() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("POST")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testDELETE() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("DELETE")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testQueryParameters() {
    // Given
    URI uriWithQuery = URI.create(TEST_URI + "?prefix=test&max-keys=100");
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(uriWithQuery)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri().getHost()).isEqualTo(uriWithQuery.getHost());
    assertThat(response.uri().getPath()).isEqualTo(uriWithQuery.getPath());
    assertThat(response.uri().getQuery()).contains("prefix=test");
    assertThat(response.uri().getQuery()).contains("max-keys=100");
    assertHeaders(response);
  }

  @Test
  void testHeaders() {
    // Given
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(
                Map.of(
                    "Host", List.of(TEST_HOST),
                    "x-amz-content-sha256", List.of("UNSIGNED-PAYLOAD"),
                    "Content-Type", List.of("application/json"),
                    "User-Agent", List.of("test-client/1.0")))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
    assertThat(response.headers().get("x-amz-content-sha256").getFirst())
        .isEqualTo("UNSIGNED-PAYLOAD");
    assertThat(response.headers().get("Content-Type").getFirst()).isEqualTo("application/json");
    assertThat(response.headers().get("User-Agent").getFirst()).isEqualTo("test-client/1.0");
  }

  private static void assertHeaders(S3SignResponse response) {
    assertThat(response.headers()).isNotEmpty();
    assertThat(response.headers()).containsKey("X-Amz-Date");
    assertThat(response.headers()).containsKey("x-amz-content-sha256");
    assertThat(response.headers().get("x-amz-content-sha256").getFirst())
        .isEqualTo("UNSIGNED-PAYLOAD"); // Fixed for S3
    assertThat(response.headers()).containsKey("Authorization");
    assertThat(response.headers().get("Authorization")).hasSize(1);
    String authHeader = response.headers().get("Authorization").getFirst();
    assertThat(authHeader).startsWith("AWS4-HMAC-SHA256 Credential=" + TEST_ACCESS_KEY);
    assertThat(authHeader).contains("SignedHeaders=");
    assertThat(authHeader).contains("Signature=");
  }
}
