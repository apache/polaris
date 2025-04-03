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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
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
    PolarisResolvedPathWrapper resolvedPolicyEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);

    PolicyEntity entity =
        PolicyEntity.of(
            resolvedPolicyEntities == null ? null : resolvedPolicyEntities.getRawLeafEntity());

    if (entity == null) {
      PolicyType policyType = PolicyType.fromName(type);
      if (policyType == null) {
        throw new BadRequestException("Unknown policy type: %s", type);
      }

      entity =
          new PolicyEntity.Builder(
                  policyIdentifier.getNamespace(), policyIdentifier.getName(), policyType)
              .setCatalogId(catalogId)
              .setDescription(description)
              .setContent(content)
              .setId(getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
              .build();

      PolicyValidators.validate(entity);

    } else {
      throw new AlreadyExistsException("Policy already exists %s", policyIdentifier);
    }

    return constructPolicy(createPolicyEntity(policyIdentifier, entity));
  }

  public List<PolicyIdentifier> listPolicies(Namespace namespace, PolicyType policyType) {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new IllegalStateException(
          String.format("Failed to fetch resolved namespace '%s'", namespace));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolicyEntity> policyEntities =
        getMetaStoreManager()
            .listEntities(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                PolarisEntityType.POLICY,
                PolarisEntitySubType.ANY_SUBTYPE)
            .getEntities()
            .stream()
            .map(
                polarisEntityActiveRecord ->
                    PolicyEntity.of(
                        getMetaStoreManager()
                            .loadEntity(
                                getCurrentPolarisContext(),
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

    PolicyEntity.Builder newPolicyBuilder = new PolicyEntity.Builder(policy);
    int policyVersion = policy.getPolicyVersion();
    if (currentPolicyVersion != policyVersion) {
      throw new PolicyVersionMismatchException(
          String.format("Policy version mismatch. Current version is %d", policyVersion));
    }
    boolean hasUpdate = false;
    if (newContent != null) {
      newPolicyBuilder.setContent(newContent);
      hasUpdate = true;
    }

    if (newDescription != null) {
      newPolicyBuilder.setDescription(newDescription);
      hasUpdate = true;
    }

    if (!hasUpdate) {
      return constructPolicy(policy);
    }

    newPolicyBuilder.setPolicyVersion(policyVersion + 1);
    PolicyEntity newPolicyEntity = newPolicyBuilder.build();
    PolicyValidators.validate(newPolicyEntity);
    newPolicyEntity = PolicyEntity.of(updatePolicy(policyIdentifier, newPolicyEntity));

    return constructPolicy(newPolicyEntity);
  }

  public boolean dropPolicy(PolicyIdentifier policyIdentifier, boolean detachAll) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);
    if (resolvedEntities == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", policyIdentifier));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    DropEntityResult dropEntityResult =
        getMetaStoreManager()
            .dropEntityIfExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                leafEntity,
                Map.of(),
                false);

    return dropEntityResult.isSuccess();
  }

  private PolicyEntity createPolicyEntity(PolicyIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(identifier.getNamespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for Policy '%s'", identifier));
    }

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();
    if (entity.getParentId() <= 0) {
      entity =
          new PolarisEntity.Builder(entity)
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .build();
    }

    entity =
        new PolarisEntity.Builder(entity).setCreateTimestamp(System.currentTimeMillis()).build();

    PolarisEntity returnedEntity =
        PolarisEntity.of(
            getMetaStoreManager()
                .createEntityIfNotExists(
                    getCurrentPolarisContext(), PolarisEntity.toCoreList(catalogPath), entity));

    LOGGER.debug("Created Policy entity {} with Identifier {}", entity, identifier);
    if (returnedEntity == null) {
      throw new IllegalStateException("Failed to create Policy entity");
    }

    return PolicyEntity.of(returnedEntity);
  }

  private PolarisEntity updatePolicy(PolicyIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(identifier, entity.getType(), entity.getSubType());
    if (resolvedEntities == null) {
      throw new IllegalStateException(
          String.format("Failed to fetch resolved PolicyIdentifier '%s'", identifier));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity returnedEntity =
        Optional.ofNullable(
                getMetaStoreManager()
                    .updateEntityPropertiesIfNotChanged(
                        getCurrentPolarisContext(), PolarisEntity.toCoreList(catalogPath), entity)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new IllegalStateException("Failed to update Policy entity");
    }

    return returnedEntity;
  }

  private PolarisMetaStoreManager getMetaStoreManager() {
    return metaStoreManager;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
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
