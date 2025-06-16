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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.secrets.ServiceSecretReference;

/**
 * Persistence-layer representation of an AWS IAM service identity used by Polaris.
 *
 * <p>This class models an AWS IAM identity (either a user or role) and extends {@link
 * ServiceIdentityInfoDpo}. It is typically used internally to store both the identity metadata
 * (such as the IAM ARN) and a reference to the actual credential (e.g., via {@link
 * ServiceSecretReference}).
 *
 * <p>Instances of this class are convertible to the public API model {@link
 * AwsIamServiceIdentityInfo}.
 */
public class AwsIamServiceIdentityInfoDpo extends ServiceIdentityInfoDpo {

  @JsonCreator
  public AwsIamServiceIdentityInfoDpo(
      @JsonProperty(value = "identityInfoReference", required = false) @Nullable
          ServiceSecretReference identityInfoReference) {
    super(ServiceIdentityType.AWS_IAM.getCode(), identityInfoReference);
  }

  @Override
  public @Nonnull ServiceIdentityInfo asServiceIdentityInfoModel() {
    return AwsIamServiceIdentityInfo.builder()
        .setIdentityType(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM)
        // TODO: inject service identity info
        .setIamArn("")
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identityTypeCode", getIdentityTypeCode())
        .toString();
  }
}
