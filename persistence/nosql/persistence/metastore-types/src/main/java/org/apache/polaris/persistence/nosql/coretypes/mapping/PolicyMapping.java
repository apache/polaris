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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.policy.PolicyEntity.POLICY_CONTENT_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_DESCRIPTION_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_TYPE_CODE_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_VERSION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;

final class PolicyMapping extends BaseCatalogContentMapping<PolicyObj, PolicyObj.Builder> {
  PolicyMapping() {
    super(
        PolicyObj.TYPE,
        CatalogStateObj.TYPE,
        CATALOG_STATE_REF_NAME_PATTERN,
        PolarisEntityType.POLICY);
  }

  @Override
  public PolicyObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return PolicyObj.builder();
  }

  @Override
  void mapToObjTypeSpecific(
      PolicyObj.Builder baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(
        baseBuilder, entity, principalSecrets, properties, internalProperties);
    var policyTypeCode = properties.remove(POLICY_TYPE_CODE_KEY);
    var description = properties.remove(POLICY_DESCRIPTION_KEY);
    var content = properties.remove(POLICY_CONTENT_KEY);
    var version = properties.remove(POLICY_VERSION_KEY);

    baseBuilder
        .policyType(
            PolicyType.fromCode(Integer.parseInt(policyTypeCode != null ? policyTypeCode : "0")))
        .description(Optional.ofNullable(description))
        .content(Optional.ofNullable(content))
        .policyVersion(
            version != null ? OptionalInt.of(Integer.parseInt(version)) : OptionalInt.empty());
  }

  @Override
  void mapToEntityTypeSpecific(
      PolicyObj o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    properties.put(POLICY_TYPE_CODE_KEY, Integer.toString(o.policyType().getCode()));
    o.description().ifPresent(v -> properties.put(POLICY_DESCRIPTION_KEY, v));
    o.content().ifPresent(v -> properties.put(POLICY_CONTENT_KEY, v));
    o.policyVersion().ifPresent(v -> properties.put(POLICY_VERSION_KEY, Integer.toString(v)));
  }
}
