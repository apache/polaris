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

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;

public interface PolarisPolicyMappingManager {

  /**
   * Attach a policy to a target entity, for example attach a policy to a table.
   *
   * <p>For inheritable policy, only one policy of the same type can be attached to the target. For
   * non-inheritable policy, multiple policies of the same type can be attached to the target.
   *
   * @param targetCatalogPath path to the target entity
   * @param target target entity
   * @param policyCatalogPath path to the policy entity
   * @param policy policy entity
   * @param parameters additional parameters for the attachment
   * @return The policy mapping record we created for this attachment. Will return ENTITY_NOT_FOUND
   *     if the specified target or policy does not exist. Will return
   *     POLICY_OF_SAME_TYPE_ALREADY_ATTACHED if the target already has a policy of the same type
   *     attached and the policy is inheritable.
   */
  @Nonnull
  PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters);

  /**
   * Detach a policy from a target entity
   *
   * @param catalogPath path to the target entity
   * @param target target entity
   * @param policyCatalogPath path to the policy entity
   * @param policy policy entity
   * @return The policy mapping record we detached. Will return ENTITY_NOT_FOUND if the specified
   *     target or policy does not exist. Will return POLICY_MAPPING_NOT_FOUND if the mapping cannot
   *     be found
   */
  @Nonnull
  PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy);

  /**
   * Load all policies attached to a target entity
   *
   * @param target target entity
   * @return the list of policy mapping records on the target entity. Will return ENTITY_NOT_FOUND
   *     if the specified target does not exist.
   */
  @Nonnull
  LoadPolicyMappingsResult loadPoliciesOnEntity(@Nonnull PolarisEntityCore target);

  /**
   * Load all policies of a specific type attached to a target entity
   *
   * @param target target entity
   * @param policyType the type of policy
   * @return the list of policy mapping records on the target entity. Will return ENTITY_NOT_FOUND
   *     if the specified target does not exist.
   */
  @Nonnull
  LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisEntityCore target, @Nonnull PolicyType policyType);
}
