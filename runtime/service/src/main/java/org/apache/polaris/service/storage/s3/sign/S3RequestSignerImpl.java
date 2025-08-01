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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import org.apache.iceberg.aws.s3.signer.S3SignRequest;
import org.apache.polaris.service.s3.sign.model.ImmutablePolarisS3SignResponse;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignResponse;
import org.apache.polaris.service.storage.StorageConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.services.s3.S3Client;

@ApplicationScoped
class S3RequestSignerImpl implements S3RequestSigner {

  private final AwsV4HttpSigner signer = AwsV4HttpSigner.create();

  @Inject StorageConfiguration storageConfiguration;

  @Override
  public PolarisS3SignResponse signRequest(S3SignRequest signingRequest) {

    URI uri = signingRequest.uri();
    SdkHttpMethod method = SdkHttpMethod.valueOf(signingRequest.method());

    SdkHttpFullRequest.Builder requestToSign =
        SdkHttpFullRequest.builder()
            .uri(uri)
            .protocol(uri.getScheme())
            .method(method)
            .headers(signingRequest.headers());

    AwsCredentials credentials =
        storageConfiguration
            .awsSystemCredentials()
            .orElseGet(() -> DefaultCredentialsProvider.builder().build())
            .resolveCredentials();

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

    return ImmutablePolarisS3SignResponse.builder()
        .uri(signedRequest.getUri())
        .headers(signedRequest.headers())
        .build();
  }
}
