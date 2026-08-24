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
package org.apache.polaris.service.it;

import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.UUID;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRestCatalogIntegrationBase;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.floci.aws.FlociAws;
import org.apache.polaris.test.floci.aws.FlociAwsAccess;
import org.apache.polaris.test.floci.aws.FlociAwsTestResource;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(MinioRustProfile.class)
@QuarkusTestResource(
    value = FlociAwsTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY),
      @ResourceArg(name = "bucket", value = "floci-s3-test-polaris"),
      @ResourceArg(name = "region", value = PolarisRestCatalogFlociS3IT.TEST_REGION),
      @ResourceArg(name = "iamEnforcement", value = "true")
    })
@ExtendWith(PolarisIntegrationTestExtension.class)
@RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
public class PolarisRestCatalogFlociS3IT extends PolarisRestCatalogIntegrationBase {

  protected static final String BUCKET_URI_PREFIX = "/floci-s3-test-polaris";
  protected static final String TEST_REGION = "us-east-1";

  @FlociAws static FlociAwsAccess flociAwsAccess;

  private static String roleArn;
  private static String roleSetupKey;

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        .put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), flociAwsAccess.s3endpoint())
        .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), ACCESS_KEY)
        .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), SECRET_KEY);
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(true)
            .setEndpoint(flociAwsAccess.s3endpoint())
            .setStsEndpoint(flociAwsAccess.stsEndpoint())
            .setRegion(TEST_REGION)
            .setRoleArn(roleArn())
            .setKmsUnavailable(true)
            .setAllowedLocations(List.of(flociAwsAccess.s3BucketUri(BUCKET_URI_PREFIX).toString()));

    return storageConfig.build();
  }

  private static String roleArn() {
    String setupKey =
        flociAwsAccess.endpoint()
            + "|"
            + flociAwsAccess.accountId()
            + "|"
            + flociAwsAccess.bucket();
    if (roleArn == null || !setupKey.equals(roleSetupKey)) {
      String roleName = "polaris-floci-rest-catalog-" + UUID.randomUUID();
      roleArn = "arn:aws:iam::" + flociAwsAccess.accountId() + ":role/" + roleName;
      flociAwsAccess
          .iamClient()
          .createRole(b -> b.roleName(roleName).assumeRolePolicyDocument(trustPolicy()));
      flociAwsAccess
          .iamClient()
          .putRolePolicy(
              b -> b.roleName(roleName).policyName("polaris-s3-access").policyDocument(s3Policy()));
      roleSetupKey = setupKey;
    }
    return roleArn;
  }

  private static String trustPolicy() {
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
        .formatted(flociAwsAccess.accountId());
  }

  private static String s3Policy() {
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
        .formatted(flociAwsAccess.bucket(), flociAwsAccess.bucket());
  }
}
