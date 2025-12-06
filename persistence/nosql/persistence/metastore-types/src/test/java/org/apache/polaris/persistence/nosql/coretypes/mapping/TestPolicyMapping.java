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
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithProperties;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithProperty;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestPolicyMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return CatalogStateObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return CATALOG_STATE_REF_NAME_PATTERN;
  }

  @Override
  public boolean isCatalogContent() {
    return true;
  }

  @Override
  public boolean isCatalogRelated() {
    return true;
  }

  @Override
  public boolean isWithStorage() {
    return false;
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE, PolicyObj.TYPE) {
          @Override
          public PolicyObj.Builder objBuilder() {
            return PolicyObj.builder().policyType(PolicyType.fromCode(0));
          }

          @Override
          public PolarisBaseEntity.Builder entityBuilder() {
            return super.entityBuilder()
                .catalogId(123L)
                .propertiesAsMap(Map.of(POLICY_TYPE_CODE_KEY, "0"));
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return Stream.of(
                base,
                //
                new MappingSample(
                    objBuilder().from(base.obj()).policyType(PolicyType.fromCode(1)).build(),
                    entityWithProperty(base.entity(), POLICY_TYPE_CODE_KEY, "1")),
                new MappingSample(
                    objBuilder().from(base.obj()).description("description").build(),
                    entityWithProperty(base.entity(), POLICY_DESCRIPTION_KEY, "description")),
                new MappingSample(
                    objBuilder().from(base.obj()).policyVersion(42).build(),
                    entityWithProperty(base.entity(), POLICY_VERSION_KEY, "42")),
                new MappingSample(
                    objBuilder().from(base.obj()).content("content-key").build(),
                    entityWithProperty(base.entity(), POLICY_CONTENT_KEY, "content-key")),
                //
                new MappingSample(
                    objBuilder()
                        .from(base.obj())
                        .policyType(PolicyType.fromCode(1))
                        .description("description")
                        .policyVersion(42)
                        .content("content-key")
                        .content("content-key")
                        .build(),
                    entityWithProperties(
                        base.entity(),
                        Map.of(
                            POLICY_TYPE_CODE_KEY,
                            "1",
                            POLICY_DESCRIPTION_KEY,
                            "description",
                            POLICY_VERSION_KEY,
                            "42",
                            POLICY_CONTENT_KEY,
                            "content-key")))
                //
                );
          }
        });
  }
}
