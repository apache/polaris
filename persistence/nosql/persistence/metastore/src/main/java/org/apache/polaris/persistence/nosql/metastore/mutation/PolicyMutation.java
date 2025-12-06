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

package org.apache.polaris.persistence.nosql.metastore.mutation;

import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_NOT_FOUND;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.PolicyMappingKey.fromIndexKey;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess;

public record PolicyMutation(
    Persistence persistence,
    MemoizedIndexedAccess memoizedIndexedAccess,
    long policyCatalogId,
    long policyId,
    @Nonnull PolicyType policyType,
    long targetCatalogId,
    long targetId,
    boolean doAttach,
    @Nonnull Map<String, String> parameters) {
  public PolicyAttachmentResult apply() {
    try {
      var committer =
          persistence
              .createCommitter(
                  POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class, PolicyAttachmentResult.class)
              .synchronizingLocally();
      return committer
          .commitRuntimeException(
              (state, refObjSupplier) -> {
                var refObj = refObjSupplier.get();
                var index =
                    refObj
                        .map(
                            ref ->
                                ref.policyMappings()
                                    .asUpdatableIndex(
                                        state.persistence(), POLICY_MAPPING_SERIALIZER))
                        .orElseGet(
                            () ->
                                IndexContainer.newUpdatableIndex(
                                    persistence, POLICY_MAPPING_SERIALIZER));
                var builder = PolicyMappingsObj.builder();
                refObj.ifPresent(builder::from);

                var policyCatalogAccess = memoizedIndexedAccess.catalogContent(policyCatalogId);

                var policyOptional = policyCatalogAccess.byId(policyId);
                if (policyOptional.isEmpty()) {
                  return state.noCommit(
                      new PolicyAttachmentResult(
                          BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null));
                }
                if (targetCatalogId != 0L && targetCatalogId != targetId) {
                  // catalog content, check whether the entity exists
                  var targetCatalogAccess =
                      targetCatalogId == policyCatalogId
                          ? policyCatalogAccess
                          : memoizedIndexedAccess.catalogContent(targetCatalogId);
                  var targetOptional = targetCatalogAccess.byId(targetId);
                  if (targetOptional.isEmpty()) {
                    return state.noCommit(new PolicyAttachmentResult(ENTITY_NOT_FOUND, null));
                  }
                }
                // else: against catalog, assume that it exists

                var result =
                    new PolicyAttachmentResult(
                        new PolarisPolicyMappingRecord(
                            targetCatalogId,
                            targetId,
                            policyCatalogId,
                            policyId,
                            policyType.getCode(),
                            parameters));

                var keyByPolicy =
                    new PolicyMappingsObj.KeyByPolicy(
                        policyCatalogId, policyId, policyType.getCode(), targetCatalogId, targetId);
                var keyByEntity =
                    new PolicyMappingsObj.KeyByEntity(
                        targetCatalogId, targetId, policyType.getCode(), policyCatalogId, policyId);

                var changed = false;
                if (doAttach) {
                  if (policyType.isInheritable()) {
                    // The contract says that at max one policy of the same inheritable policy type
                    // must
                    // be attached to a single entity.
                    var policyPrefixKey = keyByEntity.toPolicyTypePartialIndexKey();
                    var iter = index.iterator(policyPrefixKey, policyPrefixKey, false);
                    if (iter.hasNext()) {
                      var key = fromIndexKey(iter.next().getKey());
                      if (!(key instanceof PolicyMappingsObj.KeyByEntity existing
                          && existing.policyCatalogId() == policyCatalogId
                          && existing.policyId() == policyId)) {
                        // same policy-type attached, error-out
                        return state.noCommit(
                            new PolicyAttachmentResult(
                                POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS, null));
                      }
                    }
                  }

                  // note: parameters are only added to the "by entity" entry
                  index.put(keyByPolicy.toIndexKey(), PolicyMapping.EMPTY);
                  index.put(
                      keyByEntity.toIndexKey(),
                      PolicyMapping.builder().parameters(parameters).build());
                  changed = true;
                } else {
                  changed |= index.remove(keyByPolicy.toIndexKey());
                  changed |= index.remove(keyByEntity.toIndexKey());
                }

                if (changed) {
                  builder.policyMappings(index.toIndexed("mappings", state::writeOrReplace));
                  return state.commitResult(result, builder, refObj);
                }
                return state.noCommit(result);
              })
          .orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateReferenceHead(POLICY_MAPPINGS_REF_NAME);
    }
  }
}
