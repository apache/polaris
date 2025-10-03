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
package org.apache.polaris.core.identity.dpo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.secrets.SecretReference;

/**
 * Persistence-layer representation of an AWS IAM service identity used by Polaris.
 *
 * <p>This class stores only a {@link SecretReference} pointing to where the actual AWS credentials
 * are managed (e.g., in a secret manager or configuration store). The credentials themselves are
 * not persisted in this object.
 *
 * <p>At runtime, the reference can be used to retrieve the full {@link
 * org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential} which contains both
 * the identity metadata (e.g., IAM ARN) and the actual AWS credentials needed for authentication.
 *
 * <p>Instances of this class can be converted to the public API model {@link
 * AwsIamServiceIdentityInfo} via a {@link
 * org.apache.polaris.core.identity.provider.ServiceIdentityProvider}.
 */
public class AwsIamServiceIdentityInfoDpo extends ServiceIdentityInfoDpo {

  @JsonCreator
  public AwsIamServiceIdentityInfoDpo(
      @JsonProperty(value = "identityInfoReference", required = false) @Nullable
          SecretReference identityInfoReference) {
    super(ServiceIdentityType.AWS_IAM.getCode(), identityInfoReference);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identityTypeCode", getIdentityTypeCode())
        .toString();
  }
}
