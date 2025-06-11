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

public class ResolvedServiceIdentityTest {

  @Test
  void testResolvedAwsIamServiceIdentity() {
    ResolvedAwsIamServiceIdentity identity =
        new ResolvedAwsIamServiceIdentity(
            "arn:aws:iam::123456789012:user/polaris-iam-user",
            "access-key-id",
            "secret-access-key",
            "session-token");
    AwsIamServiceIdentityInfoDpo dpo =
        (AwsIamServiceIdentityInfoDpo) identity.asServiceIdentityInfoDpo();
    Assertions.assertThat(dpo.getIdentityType()).isEqualTo(ServiceIdentityType.AWS_IAM);
    Assertions.assertThat(dpo.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/polaris-iam-user");

    ServiceSecretReference identityInfoReference =
        new ServiceSecretReference(
            "urn:polaris-service-secret:defualt-identity-registry:my-realm:aws-iam", Map.of());
    identity.setIdentityInfoReference(identityInfoReference);
    dpo = (AwsIamServiceIdentityInfoDpo) identity.asServiceIdentityInfoDpo();
    Assertions.assertThat(dpo.getIdentityInfoReference()).isEqualTo(identityInfoReference);
  }
}
