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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.secrets.SecretReference;

/**
 * The internal persistence-object counterpart to ServiceIdentityInfo defined in the API model.
 * Important: JsonSubTypes must be kept in sync with {@link ServiceIdentityType}.
 *
 * <p>During the runtime, it will be resolved to an actual ResolvedServiceIdentityInfo object which
 * contains the actual service identity info and the corresponding credential.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "identityTypeCode")
@JsonSubTypes({@JsonSubTypes.Type(value = AwsIamServiceIdentityInfoDpo.class, name = "1")})
public abstract class ServiceIdentityInfoDpo {

  @JsonProperty(value = "identityTypeCode")
  private final int identityTypeCode;

  @JsonProperty(value = "identityInfoReference")
  private final SecretReference identityInfoReference;

  public ServiceIdentityInfoDpo(
      @JsonProperty(value = "identityTypeCode", required = true) int identityTypeCode,
      @JsonProperty(value = "identityInfoReference", required = false) @Nullable
          SecretReference identityInfoReference) {
    this.identityTypeCode = identityTypeCode;
    this.identityInfoReference = identityInfoReference;
  }

  public int getIdentityTypeCode() {
    return identityTypeCode;
  }

  @JsonIgnore
  public ServiceIdentityType getIdentityType() {
    return ServiceIdentityType.fromCode(identityTypeCode);
  }

  @JsonProperty
  public SecretReference getIdentityInfoReference() {
    return identityInfoReference;
  }

  /**
   * Converts this persistence object to the corresponding API model. During the conversion, some
   * fields will be dropped, e.g. the reference to the service identity's credential
   */
  public abstract @Nonnull ServiceIdentityInfo asServiceIdentityInfoModel();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identityTypeCode", getIdentityTypeCode())
        .toString();
  }
}
