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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(FlociAwsExtension.class)
public class ITFlociAwsExtension {
  private static final String REGION = "us-east-1";

  @FlociAws(
      accessKey = "myaccesskey",
      secretKey = "mysecretkey",
      bucket = "mybucket",
      region = REGION)
  private static FlociAwsAccess flociAwsStaticField;

  @FlociAws(
      accessKey = "myaccesskey",
      secretKey = "mysecretkey",
      bucket = "mybucket",
      region = REGION)
  private FlociAwsAccess flociAwsInstanceField;

  @FlociAws(bucket = "capability-iam-bucket", region = REGION, iamEnforcement = true)
  private static FlociAwsAccess iamFlociAws;

  @Test
  public void smokeTest(
      @FlociAws(
              accessKey = "myaccesskey",
              secretKey = "mysecretkey",
              bucket = "mybucket",
              region = REGION)
          FlociAwsAccess flociAws) {
    assertThat(flociAws).isSameAs(flociAwsStaticField).isSameAs(flociAwsInstanceField);
    assertThat(flociAws.hostPort()).isNotEmpty();
    assertThat(flociAws.s3endpoint()).startsWith("http");
    assertThat(flociAws.endpoint()).isEqualTo(URI.create(flociAws.s3endpoint()));
    assertThat(flociAws.bucket()).isEqualTo("mybucket");
    assertThat(flociAws.accessKey()).isEqualTo("myaccesskey");
    assertThat(flociAws.secretKey()).isEqualTo("mysecretkey");
    assertThat(flociAws.accountId()).isNotBlank();
    assertThat(flociAws.region()).contains("us-east-1");
    assertThat(flociAws.credentialsProvider()).isNotNull();
    assertThat(flociAws.stsClient()).isNotNull();
    assertThat(flociAws.iamClient()).isNotNull();
    assertThat(flociAws.kmsClient()).isNotNull();
    assertThat(flociAws.cloudWatchLogsClient()).isNotNull();
    assertThat(flociAws.stsEndpoint()).isEqualTo(flociAws.s3endpoint());
    assertThat(flociAws.iamEndpoint()).isEqualTo(flociAws.s3endpoint());
    assertThat(flociAws.kmsEndpoint()).isEqualTo(flociAws.s3endpoint());
    assertThat(flociAws.cloudWatchLogsEndpoint()).isEqualTo(flociAws.s3endpoint());

    assertThat(flociAws.icebergProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "s3.access-key-id",
                flociAws.accessKey(),
                "s3.secret-access-key",
                flociAws.secretKey(),
                "s3.endpoint",
                flociAws.s3endpoint(),
                "s3.path-style-access",
                "true",
                "http-client.type",
                "urlconnection",
                "client.region",
                "us-east-1"));

    assertThat(flociAws.hadoopConfig())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.access.key", flociAws.accessKey(),
                "fs.s3a.secret.key", flociAws.secretKey(),
                "fs.s3a.endpoint", flociAws.s3endpoint(),
                "fs.s3a.path.style.access", "true"));

    flociAws.s3put("some-key", RequestBody.fromString("hello world"));
    assertThat(flociAws.s3BucketUri("some-key"))
        .isEqualTo(URI.create("s3://" + flociAws.bucket() + "/some-key"));
    assertThat(flociAws.s3BucketUri("s3a", "some-key"))
        .isEqualTo(URI.create("s3a://" + flociAws.bucket() + "/some-key"));

    S3Client client = flociAws.s3Client();
    ResponseBytes<GetObjectResponse> object =
        client.getObjectAsBytes(b -> b.bucket(flociAws.bucket()).key("some-key"));
    assertThat(object.asUtf8String()).isEqualTo("hello world");
    assertThat(client.listObjectsV2(b -> b.bucket(flociAws.bucket())).contents())
        .anySatisfy(s3Object -> assertThat(s3Object.key()).isEqualTo("some-key"));
    client.deleteObject(b -> b.bucket(flociAws.bucket()).key("some-key"));
    assertThat(client.listObjectsV2(b -> b.bucket(flociAws.bucket())).contents()).isEmpty();
  }

  @Test
  public void injectsStaticAndInstanceFields() {
    assertThat(flociAwsStaticField).isNotNull();
    assertThat(flociAwsInstanceField).isSameAs(flociAwsStaticField);
  }

  @Test
  public void reportsOverriddenAccountId() {
    try (FlociAwsContainer container =
        new FlociAwsContainer().withDefaultAccountId("123456789012")) {
      assertThat(container.accountId()).isEqualTo("123456789012");
    }
  }

  @Test
  public void parametersReuseIdenticalConfiguration(
      @FlociAws(
              accessKey = "myaccesskey",
              secretKey = "mysecretkey",
              bucket = "mybucket",
              region = REGION)
          FlociAwsAccess param1,
      @FlociAws(
              accessKey = "myaccesskey",
              secretKey = "mysecretkey",
              bucket = "mybucket",
              region = REGION)
          FlociAwsAccess param2) {
    assertThat(param1).isSameAs(param2).isSameAs(flociAwsStaticField);
  }

  @Test
  public void s3SupportsPrefixScopedObjectOperations() {
    flociAwsStaticField.s3put("allowed/data.txt", RequestBody.fromString("allowed", UTF_8));
    flociAwsStaticField.s3put("other/data.txt", RequestBody.fromString("other", UTF_8));

    assertThat(
            flociAwsStaticField
                .s3Client()
                .listObjectsV2(b -> b.bucket(flociAwsStaticField.bucket()).prefix("allowed/"))
                .contents())
        .singleElement()
        .satisfies(s3Object -> assertThat(s3Object.key()).isEqualTo("allowed/data.txt"));
    assertThat(
            flociAwsStaticField
                .s3Client()
                .getObjectAsBytes(
                    b -> b.bucket(flociAwsStaticField.bucket()).key("allowed/data.txt"))
                .asUtf8String())
        .isEqualTo("allowed");

    flociAwsStaticField
        .s3Client()
        .deleteObject(b -> b.bucket(flociAwsStaticField.bucket()).key("allowed/data.txt"));
    flociAwsStaticField
        .s3Client()
        .deleteObject(b -> b.bucket(flociAwsStaticField.bucket()).key("other/data.txt"));
  }

  @Test
  public void iamCanCreateRoleAndInlinePolicy() {
    String roleName = newName("polaris-floci-role");
    createRoleWithS3Access(iamFlociAws, roleName);

    assertThat(iamFlociAws.iamClient().getRole(b -> b.roleName(roleName)).role().roleName())
        .isEqualTo(roleName);
    assertThat(
            iamFlociAws
                .iamClient()
                .getRolePolicy(b -> b.roleName(roleName).policyName("polaris-s3-access"))
                .policyName())
        .isEqualTo("polaris-s3-access");
  }

  @Test
  public void stsAssumeRoleCredentialsCanAccessS3() {
    String roleName = newName("polaris-floci-sts");
    createRoleWithS3Access(iamFlociAws, roleName);

    var credentials = assumeRole(iamFlociAws, roleArn(iamFlociAws, roleName), null);
    try (S3Client s3 = s3Client(iamFlociAws, credentials)) {
      s3.putObject(
          b -> b.bucket(iamFlociAws.bucket()).key("sts/full-access.txt"),
          RequestBody.fromString("sts", UTF_8));
      assertThat(
              s3.getObjectAsBytes(b -> b.bucket(iamFlociAws.bucket()).key("sts/full-access.txt"))
                  .asUtf8String())
          .isEqualTo("sts");
    }
  }

  @Test
  public void stsInlineSessionPolicyScopesS3ObjectAccess() {
    String roleName = newName("polaris-floci-scoped");
    createRoleWithS3Access(iamFlociAws, roleName);

    var credentials =
        assumeRole(
            iamFlociAws, roleArn(iamFlociAws, roleName), s3PrefixPolicy(iamFlociAws, "allowed/"));
    try (S3Client s3 = s3Client(iamFlociAws, credentials)) {
      s3.putObject(
          b -> b.bucket(iamFlociAws.bucket()).key("allowed/data.txt"),
          RequestBody.fromString("allowed", UTF_8));
      assertThat(
              s3.getObjectAsBytes(b -> b.bucket(iamFlociAws.bucket()).key("allowed/data.txt"))
                  .asUtf8String())
          .isEqualTo("allowed");

      assertThatThrownBy(
              () ->
                  s3.putObject(
                      b -> b.bucket(iamFlociAws.bucket()).key("denied/data.txt"),
                      RequestBody.fromString("denied", UTF_8)))
          .isInstanceOf(S3Exception.class);
    }
  }

  @Test
  @Disabled(
      "Blocked by https://github.com/floci-io/floci/pull/1748: "
          + "S3 ListBucket IAM condition context is not available in the pinned Floci image")
  public void stsInlineSessionPolicyScopesS3ListPrefix() {
    String roleName = newName("polaris-floci-list-scoped");
    createRoleWithS3Access(iamFlociAws, roleName);

    var credentials =
        assumeRole(
            iamFlociAws, roleArn(iamFlociAws, roleName), s3PrefixPolicy(iamFlociAws, "allowed/"));
    try (S3Client s3 = s3Client(iamFlociAws, credentials)) {
      s3.putObject(
          b -> b.bucket(iamFlociAws.bucket()).key("allowed/data.txt"),
          RequestBody.fromString("allowed", UTF_8));
      assertThat(
              s3.listObjectsV2(b -> b.bucket(iamFlociAws.bucket()).prefix("allowed/")).contents())
          .extracting(s3Object -> s3Object.key())
          .contains("allowed/data.txt");
      assertThatThrownBy(
              () -> s3.listObjectsV2(b -> b.bucket(iamFlociAws.bucket()).prefix("denied/")))
          .isInstanceOf(S3Exception.class);
    }
  }

  @Test
  public void kmsSupportsBasicKeyAndDataKeyOperations() {
    var createKeyResponse =
        flociAwsStaticField
            .kmsClient()
            .createKey(b -> b.description("Polaris Floci capability test key"));
    assertThat(createKeyResponse.keyMetadata().keyId()).isNotBlank();
    assertThat(createKeyResponse.keyMetadata().arn()).contains(":kms:");

    var dataKeyResponse =
        flociAwsStaticField
            .kmsClient()
            .generateDataKey(
                b -> b.keyId(createKeyResponse.keyMetadata().keyId()).keySpec(DataKeySpec.AES_256));
    assertThat(dataKeyResponse.ciphertextBlob().asByteArray()).isNotEmpty();
    assertThat(dataKeyResponse.plaintext().asByteArray()).isNotEmpty();

    var decryptResponse =
        flociAwsStaticField
            .kmsClient()
            .decrypt(b -> b.ciphertextBlob(dataKeyResponse.ciphertextBlob()));
    assertThat(decryptResponse.plaintext().asByteArray())
        .isEqualTo(dataKeyResponse.plaintext().asByteArray());
  }

  @Test
  public void cloudWatchLogsSupportsEventRoundTrip() {
    String logGroup = newName("polaris-floci-log-group");
    String logStream = newName("polaris-floci-log-stream");
    String message = "polaris-floci-cloudwatch-" + UUID.randomUUID();

    flociAwsStaticField.cloudWatchLogsClient().createLogGroup(b -> b.logGroupName(logGroup));
    flociAwsStaticField
        .cloudWatchLogsClient()
        .createLogStream(b -> b.logGroupName(logGroup).logStreamName(logStream));
    flociAwsStaticField
        .cloudWatchLogsClient()
        .putLogEvents(
            b ->
                b.logGroupName(logGroup)
                    .logStreamName(logStream)
                    .logEvents(
                        InputLogEvent.builder()
                            .timestamp(Instant.now().toEpochMilli())
                            .message(message)
                            .build()));

    assertThat(
            flociAwsStaticField
                .cloudWatchLogsClient()
                .describeLogGroups(b -> b.logGroupNamePrefix(logGroup))
                .logGroups())
        .singleElement()
        .satisfies(group -> assertThat(group.logGroupName()).isEqualTo(logGroup));
    assertThat(
            flociAwsStaticField
                .cloudWatchLogsClient()
                .describeLogStreams(b -> b.logGroupName(logGroup).logStreamNamePrefix(logStream))
                .logStreams())
        .singleElement()
        .satisfies(stream -> assertThat(stream.logStreamName()).isEqualTo(logStream));
    assertThat(
            flociAwsStaticField
                .cloudWatchLogsClient()
                .getLogEvents(b -> b.logGroupName(logGroup).logStreamName(logStream))
                .events())
        .singleElement()
        .satisfies(event -> assertThat(event.message()).isEqualTo(message));
  }

  private static void createRoleWithS3Access(FlociAwsAccess flociAws, String roleName) {
    flociAws
        .iamClient()
        .createRole(b -> b.roleName(roleName).assumeRolePolicyDocument(trustPolicy(flociAws)));
    flociAws
        .iamClient()
        .putRolePolicy(
            b ->
                b.roleName(roleName)
                    .policyName("polaris-s3-access")
                    .policyDocument(s3FullBucketPolicy(flociAws)));
  }

  private static AwsSessionCredentials assumeRole(
      FlociAwsAccess flociAws, String roleArn, String policy) {
    var builder =
        software.amazon.awssdk.services.sts.model.AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName("polaris-floci-session");
    if (policy != null) {
      builder.policy(policy);
    }
    var credentials = flociAws.stsClient().assumeRole(builder.build()).credentials();
    assertThat(credentials.accessKeyId()).isNotBlank();
    assertThat(credentials.secretAccessKey()).isNotBlank();
    assertThat(credentials.sessionToken()).isNotBlank();
    return AwsSessionCredentials.create(
        credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
  }

  private static S3Client s3Client(FlociAwsAccess flociAws, AwsSessionCredentials credentials) {
    return S3Client.builder()
        .endpointOverride(flociAws.endpoint())
        .forcePathStyle(true)
        .region(Region.of(flociAws.region().orElse(REGION)))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build();
  }

  private static String trustPolicy(FlociAwsAccess flociAws) {
    return """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "AWS": "arn:aws:iam::%s:root"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        }
        """
        .formatted(flociAws.accountId());
  }

  private static String s3FullBucketPolicy(FlociAwsAccess flociAws) {
    return """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
              ],
              "Resource": "arn:aws:s3:::%s/*"
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
              ],
              "Resource": "arn:aws:s3:::%s"
            }
          ]
        }
        """
        .formatted(flociAws.bucket(), flociAws.bucket());
  }

  private static String s3PrefixPolicy(FlociAwsAccess flociAws, String prefix) {
    return """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
              ],
              "Resource": "arn:aws:s3:::%s/%s*"
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
              ],
              "Resource": "arn:aws:s3:::%s",
              "Condition": {
                "StringLike": {
                  "s3:prefix": "%s*"
                }
              }
            }
          ]
        }
        """
        .formatted(flociAws.bucket(), prefix, flociAws.bucket(), prefix);
  }

  private static String roleArn(FlociAwsAccess flociAws, String roleName) {
    return "arn:aws:iam::" + flociAws.accountId() + ":role/" + roleName;
  }

  private static String newName(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }
}
