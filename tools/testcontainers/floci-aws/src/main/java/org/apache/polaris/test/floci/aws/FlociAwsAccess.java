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

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

public interface FlociAwsAccess {
  String hostPort();

  String accessKey();

  String secretKey();

  String bucket();

  String accountId();

  Optional<String> region();

  String s3endpoint();

  URI endpoint();

  AwsCredentialsProvider credentialsProvider();

  S3Client s3Client();

  StsClient stsClient();

  IamClient iamClient();

  KmsClient kmsClient();

  CloudWatchLogsClient cloudWatchLogsClient();

  default String stsEndpoint() {
    return endpoint().toString();
  }

  default String iamEndpoint() {
    return endpoint().toString();
  }

  default String kmsEndpoint() {
    return endpoint().toString();
  }

  default String cloudWatchLogsEndpoint() {
    return endpoint().toString();
  }

  Map<String, String> icebergProperties();

  Map<String, String> hadoopConfig();

  URI s3BucketUri(String path);

  URI s3BucketUri(String scheme, String path);

  @SuppressWarnings("resource")
  default void s3put(String key, RequestBody body) {
    s3Client().putObject(b -> b.bucket(bucket()).key(key), body);
  }
}
