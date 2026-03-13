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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.apache.polaris.service.storage.sign.RemoteSigner;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.services.s3.S3Client;

@ApplicationScoped
@Identifier("S3")
public class S3RemoteSigner implements RemoteSigner {

  /**
   * Headers that are excluded from signing.
   *
   * <p>Only "Host" and "x-amz-*" headers are required to be signed, other headers are optional.
   * Also, "request-scoped" headers should never be signed, because they could break the S3 signer
   * client's cache if a request is served from the cache, but contains different values for any
   * signed header.
   *
   * @see <a
   *     href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html">Create
   *     a signed AWS API request</a>
   */
  private static final Set<String> UNSIGNED_HEADERS =
      Set.of(
          // Excluded by software.amazon.awssdk.http.auth.aws.internal.signer.V4CanonicalRequest
          "connection",
          "expect",
          "transfer-encoding",
          "user-agent",
          "x-amzn-trace-id",
          "x-forwarded-for",
          // S3-specific headers
          "range",
          // Conditional headers
          "if-match",
          "if-modified-since",
          "if-none-match",
          "if-unmodified-since",
          // Transient headers
          "keep-alive",
          "proxy-authenticate",
          "proxy-authorization",
          "referer",
          "te",
          "trailer",
          "upgrade");

  @VisibleForTesting
  static final Predicate<String> UNSIGNED_HEADERS_PREDICATE =
      h -> {
        String name = h.toLowerCase(Locale.ROOT);
        return name.startsWith("amz-sdk-") || UNSIGNED_HEADERS.contains(name);
      };

  @VisibleForTesting
  static final Predicate<String> SIGNED_HEADERS_PREDICATE = UNSIGNED_HEADERS_PREDICATE.negate();

  private final AwsV4HttpSigner signer = AwsV4HttpSigner.create();

  @Inject StorageConfiguration storageConfiguration;

  @Override
  public RemoteSignResponse signRequest(RemoteSignRequest signingRequest) {

    URI uri = signingRequest.uri();
    SdkHttpMethod method = SdkHttpMethod.valueOf(signingRequest.method());

    Map<String, List<String>> signedHeaders =
        signingRequest.headers().entrySet().stream()
            .filter(e -> SIGNED_HEADERS_PREDICATE.test(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<String, List<String>> unsignedHeaders =
        signingRequest.headers().entrySet().stream()
            .filter(e -> UNSIGNED_HEADERS_PREDICATE.test(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    SdkHttpFullRequest.Builder requestToSign =
        SdkHttpFullRequest.builder()
            .uri(uri)
            .protocol(uri.getScheme())
            .method(method)
            .headers(signedHeaders);

    AwsCredentials credentials = storageConfiguration.stsCredentials().resolveCredentials();

    SignRequest.Builder<AwsCredentials> signRequest =
        SignRequest.builder(credentials)
            .request(requestToSign.build())
            .putProperty(AwsV4HttpSigner.REGION_NAME, signingRequest.region())
            .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, S3Client.SERVICE_NAME)
            .putProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE, false)
            .putProperty(AwsV4HttpSigner.NORMALIZE_PATH, false)
            .putProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED, false)
            .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false);

    String body = signingRequest.body();
    if (body != null) {
      signRequest.payload(ContentStreamProvider.fromUtf8String(body));
    }

    SignedRequest signed = signer.sign(signRequest.build());
    SdkHttpRequest signedRequest = signed.request();

    return ImmutableRemoteSignResponse.builder()
        .uri(signedRequest.getUri())
        .headers(
            ImmutableMap.<String, List<String>>builder()
                .putAll(signedRequest.headers())
                .putAll(unsignedHeaders)
                .build())
        .build();
  }
}
