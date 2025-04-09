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
package org.apache.polaris.service.catalog.policy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.policy.exceptions.PolicyVersionMismatchException;
import org.apache.polaris.core.policy.validator.PolicyValidators;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolicyCatalog.class);

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private long catalogId = -1;
  private PolarisMetaStoreManager metaStoreManager;

  public PolicyCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity =
        CatalogEntity.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
  }

  public Policy createPolicy(
      PolicyIdentifier policyIdentifier, String type, String description, String content) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(policyIdentifier.getNamespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for Policy '%s'", policyIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    PolarisResolvedPathWrapper resolvedPolicyEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);

    PolicyEntity entity =
        PolicyEntity.of(
            resolvedPolicyEntities == null ? null : resolvedPolicyEntities.getRawLeafEntity());

    if (entity != null) {
      throw new AlreadyExistsException("Policy already exists %s", policyIdentifier);
    }

    PolicyType policyType = PolicyType.fromName(type);
    if (policyType == null) {
      throw new BadRequestException("Unknown policy type: %s", type);
    }

    entity =
        new PolicyEntity.Builder(
                policyIdentifier.getNamespace(), policyIdentifier.getName(), policyType)
            .setCatalogId(catalogId)
            .setParentId(resolvedParent.getRawLeafEntity().getId())
            .setDescription(description)
            .setContent(content)
            .setId(
                metaStoreManager.generateNewEntityId(callContext.getPolarisCallContext()).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    PolicyValidators.validate(entity);

    EntityResult res =
        metaStoreManager.createEntityIfNotExists(
            callContext.getPolarisCallContext(), PolarisEntity.toCoreList(catalogPath), entity);

    if (!res.isSuccess()) {

      switch (res.getReturnStatus()) {
        case ENTITY_ALREADY_EXISTS:
          throw new AlreadyExistsException("Policy already exists %s", policyIdentifier);

        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  policyIdentifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }

    PolicyEntity resultEntity = PolicyEntity.of(res.getEntity());
    LOGGER.debug(
        "Created Policy entity {} with PolicyIdentifier {}", resultEntity, policyIdentifier);
    return constructPolicy(resultEntity);
  }

  public List<PolicyIdentifier> listPolicies(Namespace namespace, PolicyType policyType) {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new IllegalStateException(
          String.format("Failed to fetch resolved namespace '%s'", namespace));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolicyEntity> policyEntities =
        metaStoreManager
            .listEntities(
                callContext.getPolarisCallContext(),
                PolarisEntity.toCoreList(catalogPath),
                PolarisEntityType.POLICY,
                PolarisEntitySubType.NULL_SUBTYPE)
            .getEntities()
            .stream()
            .map(
                polarisEntityActiveRecord ->
                    PolicyEntity.of(
                        metaStoreManager
                            .loadEntity(
                                callContext.getPolarisCallContext(),
                                polarisEntityActiveRecord.getCatalogId(),
                                polarisEntityActiveRecord.getId(),
                                polarisEntityActiveRecord.getType())
                            .getEntity()))
            .filter(
                policyEntity -> policyType == null || policyEntity.getPolicyType() == policyType)
            .toList();

    List<PolarisEntity.NameAndId> entities =
        policyEntities.stream().map(PolarisEntity::nameAndId).toList();

    return entities.stream()
        .map(
            entity ->
                PolicyIdentifier.builder()
                    .setNamespace(namespace)
                    .setName(entity.getName())
                    .build())
        .toList();
  }

  public Policy loadPolicy(PolicyIdentifier policyIdentifier) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);

    PolicyEntity policy =
        PolicyEntity.of(resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());

    if (policy == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", policyIdentifier));
    }
    return constructPolicy(policy);
  }

  public Policy updatePolicy(
      PolicyIdentifier policyIdentifier,
      String newDescription,
      String newContent,
      int currentPolicyVersion) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);

    PolicyEntity policy =
        PolicyEntity.of(resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());

    if (policy == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", policyIdentifier));
    }

    // Verify that the current version of the policy matches the version that the user is trying to
    // update
    int policyVersion = policy.getPolicyVersion();
    if (currentPolicyVersion != policyVersion) {
      throw new PolicyVersionMismatchException(
          String.format(
              "Policy version mismatch. Given version is %d, current version is %d",
              currentPolicyVersion, policyVersion));
    }

    if (newDescription.equals(policy.getDescription()) && newContent.equals(policy.getContent())) {
      // No need to update the policy if the new description and content are the same as the current
      return constructPolicy(policy);
    }

    PolicyEntity.Builder newPolicyBuilder = new PolicyEntity.Builder(policy);
    newPolicyBuilder.setContent(newContent);
    newPolicyBuilder.setDescription(newDescription);
    newPolicyBuilder.setPolicyVersion(policyVersion + 1);
    PolicyEntity newPolicyEntity = newPolicyBuilder.build();

    PolicyValidators.validate(newPolicyEntity);

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    newPolicyEntity =
        Optional.ofNullable(
                metaStoreManager
                    .updateEntityPropertiesIfNotChanged(
                        callContext.getPolarisCallContext(),
                        PolarisEntity.toCoreList(catalogPath),
                        newPolicyEntity)
                    .getEntity())
            .map(PolicyEntity::of)
            .orElse(null);

    if (newPolicyEntity == null) {
      throw new IllegalStateException(
          String.format("Failed to update policy %s", policyIdentifier));
    }

    return constructPolicy(newPolicyEntity);
  }

  public boolean dropPolicy(PolicyIdentifier policyIdentifier, boolean detachAll) {
    // TODO: Implement detachAll when we support attach/detach policy
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);
    if (resolvedEntities == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", policyIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    DropEntityResult dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            leafEntity,
            Map.of(),
            false);

    dropEntityResult.maybeThrowException();
    return dropEntityResult.isSuccess();
  }

  private static Policy constructPolicy(PolicyEntity policyEntity) {
    return Policy.builder()
        .setPolicyType(policyEntity.getPolicyType().getName())
        .setInheritable(policyEntity.getPolicyType().isInheritable())
        .setName(policyEntity.getName())
        .setDescription(policyEntity.getDescription())
        .setContent(policyEntity.getContent())
        .setVersion(policyEntity.getPolicyVersion())
        .build();
  }
}
