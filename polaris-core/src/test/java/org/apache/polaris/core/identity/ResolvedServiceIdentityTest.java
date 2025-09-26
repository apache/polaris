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

package org.apache.polaris.core.identity;

import java.util.Map;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.resolved.ResolvedAwsIamServiceIdentity;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class ResolvedServiceIdentityTest {

  @Test
  void testResolvedAwsIamServiceIdentity() {
    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsSessionCredentials.create("access-key", "secret-key", "session-token"));

    ResolvedAwsIamServiceIdentity identity =
        new ResolvedAwsIamServiceIdentity(
            "arn:aws:iam::123456789012:user/polaris-iam-user", awsCredentialsProvider);
    AwsIamServiceIdentityInfoDpo dpo =
        (AwsIamServiceIdentityInfoDpo) identity.asServiceIdentityInfoDpo();
    Assertions.assertThat(dpo.getIdentityType()).isEqualTo(ServiceIdentityType.AWS_IAM);

    ServiceSecretReference identityInfoReference =
        new ServiceSecretReference(
            "urn:polaris-secret:defualt-identity-registry:my-realm:aws-iam", Map.of());
    identity.setIdentityInfoReference(identityInfoReference);
    dpo = (AwsIamServiceIdentityInfoDpo) identity.asServiceIdentityInfoDpo();
    Assertions.assertThat(dpo.getIdentityInfoReference()).isEqualTo(identityInfoReference);

    AwsSessionCredentials credentials =
        (AwsSessionCredentials) identity.getAwsCredentialsProvider().resolveCredentials();
    Assertions.assertThat(credentials.accessKeyId()).isEqualTo("access-key");
    Assertions.assertThat(credentials.secretAccessKey()).isEqualTo("secret-key");
    Assertions.assertThat(credentials.sessionToken()).isEqualTo("session-token");
  }
}
