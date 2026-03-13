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

package org.apache.polaris.service.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class S3RemoteSignerTest {

  @Mock private StorageConfiguration storageConfiguration;
  @Mock private AwsCredentialsProvider awsCredentialsProvider;

  @InjectMocks private S3RemoteSigner signer;

  private static final String TEST_ACCESS_KEY = "test-access-key";
  private static final String TEST_SECRET_KEY = "test-secret-key";
  private static final String TEST_REGION = "us-west-2";
  private static final String TEST_HOST = "test-bucket.s3.us-west-2.amazonaws.com";
  private static final URI TEST_URI = URI.create("https://" + TEST_HOST + "/test-path");

  @BeforeEach
  void setUp() {
    AwsCredentials credentials = AwsBasicCredentials.create(TEST_ACCESS_KEY, TEST_SECRET_KEY);
    when(storageConfiguration.stsCredentials()).thenReturn(awsCredentialsProvider);
    when(awsCredentialsProvider.resolveCredentials()).thenReturn(credentials);
  }

  @Test
  void testGET() {
    // Given
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeadersSigned(response);
  }

  @Test
  void testPUT() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("PUT")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeadersSigned(response);
  }

  @Test
  void testPOST() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("POST")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeadersSigned(response);
  }

  @Test
  void testDELETE() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("DELETE")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeadersSigned(response);
  }

  @Test
  void testQueryParameters() {
    // Given
    URI uriWithQuery = URI.create(TEST_URI + "?prefix=test&max-keys=100");
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(uriWithQuery)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri().getHost()).isEqualTo(uriWithQuery.getHost());
    assertThat(response.uri().getPath()).isEqualTo(uriWithQuery.getPath());
    assertThat(response.uri().getQuery()).contains("prefix=test");
    assertThat(response.uri().getQuery()).contains("max-keys=100");
    assertHeadersSigned(response);
  }

  @Test
  void testHeaders() {
    // Given
    Map<String, List<String>> headers =
        Map.ofEntries(
            // Common headers included in the signature
            Map.entry("Host", List.of(TEST_HOST)),
            Map.entry("Content-Type", List.of("application/octet-stream")),
            // Headers automatically excluded by AwsV4HttpSigner
            Map.entry("Connection", List.of("keep-alive")),
            Map.entry("x-amzn-trace-id", List.of("Root=1-abc123")),
            Map.entry("User-Agent", List.of("test-client/1.0")),
            Map.entry("Expect", List.of("100-continue")),
            Map.entry("Transfer-Encoding", List.of("chunked")),
            Map.entry("X-Forwarded-For", List.of("192.168.1.1")),
            // Amazon SDK-specific debug headers (excluded from signature)
            Map.entry("amz-sdk-invocation-id", List.of("12345")),
            Map.entry("amz-sdk-retry", List.of("0/0/500")),
            // HTTP conditional headers (excluded from signature)
            Map.entry("If-Match", List.of("\"abc123\"")),
            Map.entry("If-Modified-Since", List.of("Wed, 21 Oct 2015 07:28:00 GMT")),
            Map.entry("If-None-Match", List.of("\"xyz789\"")),
            Map.entry("If-Unmodified-Since", List.of("Wed, 21 Oct 2015 07:28:00 GMT")),
            // Other headers (excluded from signature)
            Map.entry("Range", List.of("bytes=0-1023")),
            Map.entry("Referer", List.of("https://example.com")));
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(headers)
            .properties(Map.of())
            .build();

    // When
    RemoteSignResponse response = signer.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeadersSigned(response);

    assertThat(response.headers()).containsAllEntriesOf(headers);

    String authHeader = response.headers().get("Authorization").getFirst();
    String signedHeadersPart =
        authHeader.substring(
            authHeader.indexOf("SignedHeaders=") + "SignedHeaders=".length(),
            authHeader.indexOf(", Signature="));
    List<String> signedHeaders = List.of(signedHeadersPart.split(";"));

    assertThat(signedHeaders)
        .allMatch(S3RemoteSigner.SIGNED_HEADERS_PREDICATE)
        .containsAll(
            headers.keySet().stream()
                .map(String::toLowerCase)
                .filter(S3RemoteSigner.SIGNED_HEADERS_PREDICATE)
                .collect(Collectors.toSet()));
  }

  private static void assertHeadersSigned(RemoteSignResponse response) {
    assertThat(response.headers()).isNotEmpty();
    assertThat(response.headers()).containsKey("X-Amz-Date"); // added by the signer
    assertThat(response.headers()).containsKey("x-amz-content-sha256");
    assertThat(response.headers().get("x-amz-content-sha256").getFirst())
        .isEqualTo("UNSIGNED-PAYLOAD"); // Fixed when no request body
    assertThat(response.headers()).containsKey("Authorization");
    assertThat(response.headers().get("Authorization")).hasSize(1);
    String authHeader = response.headers().get("Authorization").getFirst();
    assertThat(authHeader).startsWith("AWS4-HMAC-SHA256 Credential=" + TEST_ACCESS_KEY);
    assertThat(authHeader).contains("SignedHeaders=");
    assertThat(authHeader).contains("Signature=");
  }
}
