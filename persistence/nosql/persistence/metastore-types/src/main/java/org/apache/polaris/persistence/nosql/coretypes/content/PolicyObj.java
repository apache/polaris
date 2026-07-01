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
package org.apache.polaris.persistence.nosql.coretypes.content;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

@PolarisImmutable
@JsonSerialize(as = ImmutablePolicyObj.class)
@JsonDeserialize(as = ImmutablePolicyObj.class)
public interface PolicyObj extends ContentObj {
  ObjType TYPE = new CatalogPolicyObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  PolicyType policyType();

  Optional<String> description();

  Optional<String> content();

  OptionalInt policyVersion();

  static Builder builder() {
    return ImmutablePolicyObj.builder();
  }

  final class CatalogPolicyObjType extends AbstractObjType<PolicyObj> {
    public CatalogPolicyObjType() {
      super("pol", "Policy", PolicyObj.class);
    }
  }

  @SuppressWarnings("unused")
  interface Builder extends ContentObj.Builder<PolicyObj, Builder> {
    @CanIgnoreReturnValue
    Builder from(PolicyObj from);

    @CanIgnoreReturnValue
    Builder policyType(PolicyType policyType);

    @CanIgnoreReturnValue
    Builder description(String description);

    @CanIgnoreReturnValue
    Builder description(Optional<String> description);

    @CanIgnoreReturnValue
    Builder content(String content);

    @CanIgnoreReturnValue
    Builder content(Optional<String> content);

    @CanIgnoreReturnValue
    Builder policyVersion(int policyVersion);

    @CanIgnoreReturnValue
    Builder policyVersion(OptionalInt policyVersion);
  }
}
