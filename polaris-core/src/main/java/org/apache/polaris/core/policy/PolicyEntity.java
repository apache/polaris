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
package org.apache.polaris.core.policy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;

public class PolicyEntity extends PolarisEntity {

  public static final String POLICY_TYPE_CODE_KEY = "policy-type-code";
  public static final String POLICY_DESCRIPTION_KEY = "policy-description";
  public static final String POLICY_VERSION_KEY = "policy-version";
  public static final String POLICY_CONTENT_KEY = "policy-content";

  PolicyEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.POLICY, "Invalid entity type: %s", getType());
  }

  public static @Nullable PolicyEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new PolicyEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public PolicyType getPolicyType() {
    return PolicyType.fromCode(getPolicyTypeCode());
  }

  @JsonIgnore
  public int getPolicyTypeCode() {
    String policyTypeCode = getPropertiesAsMap().get(POLICY_TYPE_CODE_KEY);
    Preconditions.checkNotNull(policyTypeCode, "Invalid policy entity: policy type must exist");
    return Integer.parseInt(policyTypeCode);
  }

  @JsonIgnore
  public String getDescription() {
    return getPropertiesAsMap().get(POLICY_DESCRIPTION_KEY);
  }

  @JsonIgnore
  public String getContent() {
    return getPropertiesAsMap().get(POLICY_CONTENT_KEY);
  }

  @JsonIgnore
  public int getPolicyVersion() {
    return Integer.parseInt(getPropertiesAsMap().get(POLICY_VERSION_KEY));
  }

  @JsonIgnore
  public Namespace getParentNamespace() {
    String parentNamespace = getInternalPropertiesAsMap().get(NamespaceEntity.PARENT_NAMESPACE_KEY);
    if (parentNamespace != null) {
      return RESTUtil.decodeNamespace(parentNamespace);
    }
    return null;
  }

  public static class Builder extends PolarisEntity.BaseBuilder<PolicyEntity, Builder> {
    public Builder(Namespace namespace, String policyName, PolicyType policyType) {
      super();
      setType(PolarisEntityType.POLICY);
      setParentNamespace(namespace);
      setName(policyName);
      setPolicyType(policyType);
      setPolicyVersion(0);
    }

    public Builder(PolicyEntity original) {
      super(original);
    }

    @Override
    public PolicyEntity build() {
      Preconditions.checkArgument(
          properties.containsKey(POLICY_TYPE_CODE_KEY), "Policy type must be specified");

      return new PolicyEntity(buildBase());
    }

    public Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public Builder setPolicyType(PolicyType policyType) {
      Preconditions.checkArgument(policyType != null, "Policy type must be specified");
      properties.put(POLICY_TYPE_CODE_KEY, Integer.toString(policyType.getCode()));
      return this;
    }

    public Builder setDescription(String description) {
      properties.put(POLICY_DESCRIPTION_KEY, description);
      return this;
    }

    public Builder setPolicyVersion(int version) {
      properties.put(POLICY_VERSION_KEY, Integer.toString(version));
      return this;
    }

    public Builder setContent(String content) {
      properties.put(POLICY_CONTENT_KEY, content);
      return this;
    }
  }
}
