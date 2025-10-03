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
package org.apache.polaris.core.identity.credential;

import java.util.Map;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class AwsIamServiceIdentityCredentialTest {

  @Test
  void testConstructorWithIamArnOnly() {
    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:user/test-user");

    Assertions.assertThat(credential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/test-user");
    Assertions.assertThat(credential.getIdentityType()).isEqualTo(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(credential.getAwsCredentialsProvider())
        .isInstanceOf(DefaultCredentialsProvider.class);
  }

  @Test
  void testConstructorWithIamArnAndCredentialsProvider() {
    StaticCredentialsProvider credProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key"));

    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential(
            "arn:aws:iam::123456789012:role/test-role", credProvider);

    Assertions.assertThat(credential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:role/test-role");
    Assertions.assertThat(credential.getAwsCredentialsProvider()).isEqualTo(credProvider);
  }

  @Test
  void testConstructorWithAllParameters() {
    ServiceSecretReference ref =
        new ServiceSecretReference("urn:polaris-secret:test:ref", Map.of());
    StaticCredentialsProvider credProvider =
        StaticCredentialsProvider.create(
            AwsSessionCredentials.create("access-key", "secret-key", "session-token"));

    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential(
            ref, "arn:aws:iam::123456789012:user/test-user", credProvider);

    Assertions.assertThat(credential.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/test-user");
    Assertions.assertThat(credential.getIdentityInfoReference()).isEqualTo(ref);
    Assertions.assertThat(credential.getAwsCredentialsProvider()).isEqualTo(credProvider);
  }

  @Test
  void testConversionToDpo() {
    ServiceSecretReference ref =
        new ServiceSecretReference("urn:polaris-secret:test:reference", Map.of());
    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential(
            ref, "arn:aws:iam::123456789012:user/test-user", DefaultCredentialsProvider.create());

    ServiceIdentityInfoDpo dpo = credential.asServiceIdentityInfoDpo();

    Assertions.assertThat(dpo).isNotNull();
    Assertions.assertThat(dpo).isInstanceOf(AwsIamServiceIdentityInfoDpo.class);
    Assertions.assertThat(dpo.getIdentityType()).isEqualTo(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(dpo.getIdentityInfoReference()).isEqualTo(ref);
  }

  @Test
  void testConversionToModel() {
    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:user/polaris-service");

    ServiceIdentityInfo model = credential.asServiceIdentityInfoModel();

    Assertions.assertThat(model).isNotNull();
    Assertions.assertThat(model).isInstanceOf(AwsIamServiceIdentityInfo.class);
    Assertions.assertThat(model.getIdentityType())
        .isEqualTo(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM);

    AwsIamServiceIdentityInfo awsModel = (AwsIamServiceIdentityInfo) model;
    Assertions.assertThat(awsModel.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-service");
  }

  @Test
  void testCredentialsProviderWithStaticBasicCredentials() {
    StaticCredentialsProvider credProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("my-key-id", "my-secret"));

    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:user/test", credProvider);

    Assertions.assertThat(credential.getAwsCredentialsProvider()).isEqualTo(credProvider);
    AwsBasicCredentials creds = (AwsBasicCredentials) credProvider.resolveCredentials();
    Assertions.assertThat(creds.accessKeyId()).isEqualTo("my-key-id");
    Assertions.assertThat(creds.secretAccessKey()).isEqualTo("my-secret");
  }

  @Test
  void testCredentialsProviderWithSessionCredentials() {
    StaticCredentialsProvider credProvider =
        StaticCredentialsProvider.create(
            AwsSessionCredentials.create("access-key", "secret-key", "session-token"));

    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:role/test", credProvider);

    Assertions.assertThat(credential.getAwsCredentialsProvider()).isEqualTo(credProvider);
    AwsSessionCredentials creds = (AwsSessionCredentials) credProvider.resolveCredentials();
    Assertions.assertThat(creds.accessKeyId()).isEqualTo("access-key");
    Assertions.assertThat(creds.secretAccessKey()).isEqualTo("secret-key");
    Assertions.assertThat(creds.sessionToken()).isEqualTo("session-token");
  }

  @Test
  void testModelDoesNotExposeCredentials() {
    // Verify that the API model contains identity info but not credentials
    StaticCredentialsProvider credProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create("secret-access-key-id", "secret-access-key"));

    AwsIamServiceIdentityCredential credential =
        new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:user/service", credProvider);

    ServiceIdentityInfo model = credential.asServiceIdentityInfoModel();
    AwsIamServiceIdentityInfo awsModel = (AwsIamServiceIdentityInfo) model;

    // Model should have the ARN
    Assertions.assertThat(awsModel.getIamArn()).isEqualTo("arn:aws:iam::123456789012:user/service");

    // Model should NOT have access keys or secrets (they're not in the AwsIamServiceIdentityInfo
    // class)
    // This is by design - credentials are never exposed in API responses
  }
}
