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

package org.apache.polaris.test.floci.aws;

import com.google.common.base.Preconditions;
import io.floci.testcontainers.FlociContainer;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.Base58;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;

public final class FlociAwsContainer extends FlociContainer
    implements FlociAwsAccess, AutoCloseable {
  private static final String DEFAULT_REGION = "us-east-1";

  private final String accessKey;
  private final String secretKey;
  private final String bucket;
  private String accountId;
  private final Optional<String> region;

  private String hostPort;
  private String endpoint;
  private SdkHttpClient httpClient;
  private S3Client s3;
  private StsClient sts;
  private IamClient iam;
  private KmsClient kms;
  private CloudWatchLogsClient cloudWatchLogs;

  public FlociAwsContainer() {
    this(null, null, null, null, null, false);
  }

  public FlociAwsContainer(
      String image,
      String accessKey,
      String secretKey,
      String bucket,
      String region,
      boolean iamEnforcement) {
    super(
        ContainerSpecHelper.containerSpecHelper("floci", FlociAwsContainer.class)
            .dockerImageName(image)
            .asCompatibleSubstituteFor("floci/floci"));
    withNetworkAliases(randomString("floci"));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(FlociAwsContainer.class)));
    this.accessKey = accessKey != null ? accessKey : getAccessKey();
    this.secretKey = secretKey != null ? secretKey : getSecretKey();
    this.bucket = bucket != null ? bucket : randomString("bucket");
    this.accountId = getDefaultAccountId();
    this.region = Optional.ofNullable(region).or(() -> Optional.of(DEFAULT_REGION));
    this.region.ifPresent(super::withRegion);
    if (iamEnforcement) {
      withIamEnforcement();
    }
  }

  @Override
  public FlociAwsContainer withRegion(String region) {
    super.withRegion(region);
    return this;
  }

  public FlociAwsContainer withIamEnforcement() {
    super.withIamConfig(c -> c.enforcementEnabled(true).seedDeployerPrincipal(true));
    return this;
  }

  @Override
  public FlociAwsContainer withDefaultAccountId(String accountId) {
    super.withDefaultAccountId(accountId);
    this.accountId = accountId;
    return this;
  }

  @Override
  public FlociAwsContainer withStartupAttempts(int attempts) {
    super.withStartupAttempts(attempts);
    return this;
  }

  @Override
  public String hostPort() {
    Preconditions.checkState(hostPort != null, "Container not yet started");
    return hostPort;
  }

  @Override
  public String accessKey() {
    return accessKey;
  }

  @Override
  public String secretKey() {
    return secretKey;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public Optional<String> region() {
    return region;
  }

  @Override
  public String accountId() {
    return accountId;
  }

  @Override
  public String s3endpoint() {
    Preconditions.checkState(endpoint != null, "Container not yet started");
    return endpoint;
  }

  @Override
  public URI endpoint() {
    return URI.create(s3endpoint());
  }

  @Override
  public AwsCredentialsProvider credentialsProvider() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey(), secretKey()));
  }

  @Override
  public S3Client s3Client() {
    Preconditions.checkState(s3 != null, "Container not yet started");
    return s3;
  }

  @Override
  public synchronized StsClient stsClient() {
    Preconditions.checkState(endpoint != null, "Container not yet started");
    if (sts == null) {
      sts = createStsClient();
    }
    return sts;
  }

  @Override
  public synchronized IamClient iamClient() {
    Preconditions.checkState(endpoint != null, "Container not yet started");
    if (iam == null) {
      iam = createIamClient();
    }
    return iam;
  }

  @Override
  public synchronized KmsClient kmsClient() {
    Preconditions.checkState(endpoint != null, "Container not yet started");
    if (kms == null) {
      kms = createKmsClient();
    }
    return kms;
  }

  @Override
  public synchronized CloudWatchLogsClient cloudWatchLogsClient() {
    Preconditions.checkState(endpoint != null, "Container not yet started");
    if (cloudWatchLogs == null) {
      cloudWatchLogs = createCloudWatchLogsClient();
    }
    return cloudWatchLogs;
  }

  @Override
  public Map<String, String> icebergProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("s3.access-key-id", accessKey());
    props.put("s3.secret-access-key", secretKey());
    props.put("s3.endpoint", s3endpoint());
    props.put("s3.path-style-access", "true");
    props.put("http-client.type", "urlconnection");
    region().ifPresent(r -> props.put("client.region", r));
    return props;
  }

  @Override
  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    r.put("fs.s3a.access.key", accessKey());
    r.put("fs.s3a.secret.key", secretKey());
    r.put("fs.s3a.endpoint", s3endpoint());
    r.put("fs.s3a.path.style.access", "true");
    return r;
  }

  @Override
  public URI s3BucketUri(String path) {
    return s3BucketUri("s3", path);
  }

  @Override
  public URI s3BucketUri(String scheme, String path) {
    return URI.create(String.format("%s://%s/", scheme, bucket())).resolve(path);
  }

  @Override
  public void start() {
    super.start();

    URI containerEndpoint = URI.create(getEndpoint());
    this.hostPort = containerEndpoint.getHost() + ":" + containerEndpoint.getPort();
    this.endpoint = containerEndpoint + "/";

    this.httpClient = UrlConnectionHttpClient.create();
    this.s3 = createS3Client();
    try {
      this.s3.createBucket(CreateBucketRequest.builder().bucket(bucket()).build());
    } catch (S3Exception e) {
      stop();
      throw e;
    }
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public void stop() {
    try {
      closeClient(cloudWatchLogs);
      closeClient(kms);
      closeClient(iam);
      closeClient(sts);
      if (s3 != null) {
        s3.close();
      }
      closeClient(httpClient);
    } finally {
      cloudWatchLogs = null;
      kms = null;
      iam = null;
      sts = null;
      s3 = null;
      endpoint = null;
      httpClient = null;
      super.stop();
    }
  }

  private S3Client createS3Client() {
    return S3Client.builder()
        .httpClient(httpClient())
        .endpointOverride(URI.create(s3endpoint()))
        .forcePathStyle(true)
        .region(Region.of(region().orElse(DEFAULT_REGION)))
        .credentialsProvider(credentialsProvider())
        .build();
  }

  private StsClient createStsClient() {
    return StsClient.builder()
        .httpClient(httpClient())
        .endpointOverride(URI.create(s3endpoint()))
        .region(Region.of(region().orElse(DEFAULT_REGION)))
        .credentialsProvider(credentialsProvider())
        .build();
  }

  private IamClient createIamClient() {
    return IamClient.builder()
        .httpClient(httpClient())
        .endpointOverride(URI.create(s3endpoint()))
        .region(Region.of(region().orElse(DEFAULT_REGION)))
        .credentialsProvider(credentialsProvider())
        .build();
  }

  private KmsClient createKmsClient() {
    return KmsClient.builder()
        .httpClient(httpClient())
        .endpointOverride(URI.create(s3endpoint()))
        .region(Region.of(region().orElse(DEFAULT_REGION)))
        .credentialsProvider(credentialsProvider())
        .build();
  }

  private CloudWatchLogsClient createCloudWatchLogsClient() {
    return CloudWatchLogsClient.builder()
        .httpClient(httpClient())
        .endpointOverride(URI.create(s3endpoint()))
        .region(Region.of(region().orElse(DEFAULT_REGION)))
        .credentialsProvider(credentialsProvider())
        .build();
  }

  private SdkHttpClient httpClient() {
    Preconditions.checkState(httpClient != null, "Container not yet started");
    return httpClient;
  }

  private static void closeClient(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        LoggerFactory.getLogger(FlociAwsContainer.class).warn("Failed to close AWS client", e);
      }
    }
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }
}
