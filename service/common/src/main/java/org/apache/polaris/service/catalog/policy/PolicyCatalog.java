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

import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS;
import static org.apache.polaris.service.types.PolicyAttachmentTarget.TypeEnum.CATALOG;

import com.google.common.base.Strings;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.policy.exceptions.PolicyVersionMismatchException;
import org.apache.polaris.core.policy.validator.PolicyValidators;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
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
    var resolvedPolicyPath = getResolvedPathWrapper(policyIdentifier);
    var policy = PolicyEntity.of(resolvedPolicyPath.getRawLeafEntity());
    return constructPolicy(policy);
  }

  public Policy updatePolicy(
      PolicyIdentifier policyIdentifier,
      String newDescription,
      String newContent,
      int currentPolicyVersion) {
    var resolvedPolicyPath = getResolvedPathWrapper(policyIdentifier);
    var policy = PolicyEntity.of(resolvedPolicyPath.getRawLeafEntity());

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

    List<PolarisEntity> catalogPath = resolvedPolicyPath.getRawParentPath();
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
    var resolvedPolicyPath = getResolvedPathWrapper(policyIdentifier);
    var catalogPath = resolvedPolicyPath.getRawParentPath();
    var policyEntity = resolvedPolicyPath.getRawLeafEntity();

    return metaStoreManager
        .dropEntityIfExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            policyEntity,
            Map.of(),
            false)
        .isSuccess();
  }

  public boolean attachPolicy(
      PolicyIdentifier policyIdentifier,
      PolicyAttachmentTarget target,
      Map<String, String> parameters) {

    var resolvedPolicyPath = getResolvedPathWrapper(policyIdentifier);
    var policyCatalogPath = PolarisEntity.toCoreList(resolvedPolicyPath.getRawParentPath());
    var policyEntity = PolicyEntity.of(resolvedPolicyPath.getRawLeafEntity());

    var resolvedTargetPath = getResolvedPathWrapper(target);
    var targetCatalogPath = PolarisEntity.toCoreList(resolvedTargetPath.getRawParentPath());
    var targetEntity = resolvedTargetPath.getRawLeafEntity();

    PolicyValidators.validateAttach(policyEntity, targetEntity);

    var result =
        metaStoreManager.attachPolicyToEntity(
            callContext.getPolarisCallContext(),
            targetCatalogPath,
            targetEntity,
            policyCatalogPath,
            policyEntity,
            parameters);

    if (result.getReturnStatus() == POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS) {
      var targetId = catalogEntity.getName();
      if (target.getType() != CATALOG) {
        targetId += "." + String.join(".", target.getPath());
      }
      throw new PolicyMappingAlreadyExistsException(
          "The policy mapping of same type(%s) for %s already exists",
          policyEntity.getPolicyType().getName(), targetId);
    }

    return result.isSuccess();
  }

  public boolean detachPolicy(PolicyIdentifier policyIdentifier, PolicyAttachmentTarget target) {
    var resolvedPolicyPath = getResolvedPathWrapper(policyIdentifier);
    var policyCatalogPath = PolarisEntity.toCoreList(resolvedPolicyPath.getRawParentPath());
    var policyEntity = PolicyEntity.of(resolvedPolicyPath.getRawLeafEntity());

    var resolvedTargetPath = getResolvedPathWrapper(target);
    var targetCatalogPath = PolarisEntity.toCoreList(resolvedTargetPath.getRawParentPath());
    var targetEntity = resolvedTargetPath.getRawLeafEntity();

    return metaStoreManager
        .detachPolicyFromEntity(
            callContext.getPolarisCallContext(),
            targetCatalogPath,
            targetEntity,
            policyCatalogPath,
            policyEntity)
        .isSuccess();
  }

  public List<Policy> getApplicablePolicies(
      Namespace namespace, String targetName, PolicyType policyType) {
    var targetFullPath = getFullPath(namespace, targetName);
    return getEffectivePolicies(targetFullPath, policyType);
  }

  /**
   * Returns the effective policies for a given hierarchical path and policy type.
   *
   * <p>Potential Performance Improvements:
   *
   * <ul>
   *   <li>Range Query Optimization: Enhance the query mechanism to fetch policies for all entities
   *       in a single range query, reducing the number of individual queries against the mapping
   *       table.
   *   <li>Filtering on Inheritable: Improve the filtering process by applying the inheritable
   *       condition at the data retrieval level, so that only the relevant policies for non-leaf
   *       nodes are processed.
   *   <li>Caching: Implement caching for up-level policies to avoid redundant calculations and
   *       lookups, especially for frequently accessed paths.
   * </ul>
   *
   * @param path the list of entities representing the hierarchical path
   * @param policyType the type of policy to filter on
   * @return a list of effective policies, combining inherited policies from upper levels and
   *     non-inheritable policies from the final entity
   */
  private List<Policy> getEffectivePolicies(List<PolarisEntity> path, PolicyType policyType) {
    if (path == null || path.isEmpty()) {
      return List.of();
    }

    Map<String, Policy> inheritedPolicies = new LinkedHashMap<>();
    // Final list of effective policies (inheritable + last-entity non-inheritable)
    List<Policy> finalPolicies = new ArrayList<>();

    // Process all entities except the last one
    for (int i = 0; i < path.size() - 1; i++) {
      PolarisEntity entity = path.get(i);
      List<Policy> currentPolicies = getPolicies(entity, policyType);

      for (Policy policy : currentPolicies) {
        // For non-last entities, we only carry forward inheritable policies
        if (policy.getInheritable()) {
          // Put in map; overwrites by policyType if encountered again
          inheritedPolicies.put(policy.getPolicyType(), policy);
        }
      }
    }

    // Now handle the last entity's policies
    List<Policy> lastPolicies = getPolicies(path.getLast(), policyType);

    for (Policy policy : lastPolicies) {
      if (policy.getInheritable()) {
        // Overwrite anything by the same policyType in the inherited map
        inheritedPolicies.put(policy.getPolicyType(), policy);
      } else {
        // Non-inheritable => goes directly to final list
        finalPolicies.add(policy);
      }
    }

    // Append all inherited policies at the end, preserving insertion order
    finalPolicies.addAll(inheritedPolicies.values());

    return finalPolicies;
  }

  private List<Policy> getPolicies(PolarisEntity target, PolicyType policyType) {
    LoadPolicyMappingsResult result;
    if (policyType == null) {
      result = metaStoreManager.loadPoliciesOnEntity(callContext.getPolarisCallContext(), target);
    } else {
      result =
          metaStoreManager.loadPoliciesOnEntityByType(
              callContext.getPolarisCallContext(), target, policyType);
    }

    return result.getEntities().stream().map(PolicyCatalog::toPolicy).toList();
  }

  private List<PolarisEntity> getFullPath(Namespace namespace, String targetName) {
    if (namespace == null || namespace.isEmpty()) {
      // catalog
      return List.of(catalogEntity);
    } else if (Strings.isNullOrEmpty(targetName)) {
      // namespace
      var resolvedTargetEntity = resolvedEntityView.getResolvedPath(namespace);
      if (resolvedTargetEntity == null) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      return resolvedTargetEntity.getRawFullPath();
    } else {
      // table
      var tableIdentifier = TableIdentifier.of(namespace, targetName);
      // only Iceberg tables are supported
      var resolvedTableEntity =
          resolvedEntityView.getResolvedPath(
              tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
      if (resolvedTableEntity == null) {
        throw new NoSuchTableException("Iceberg Table does not exist: %s", tableIdentifier);
      }
      return resolvedTableEntity.getRawFullPath();
    }
  }

  private PolarisResolvedPathWrapper getResolvedPathWrapper(PolicyIdentifier policyIdentifier) {
    var resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            policyIdentifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);
    if (resolvedEntities == null || resolvedEntities.getResolvedLeafEntity() == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", policyIdentifier));
    }
    return resolvedEntities;
  }

  private PolarisResolvedPathWrapper getResolvedPathWrapper(
      @Nonnull PolicyAttachmentTarget target) {
    return switch (target.getType()) {
      // get the current catalog entity, since policy cannot apply across catalog at this moment
      case CATALOG -> resolvedEntityView.getResolvedReferenceCatalogEntity();
      case NAMESPACE -> {
        var namespace = Namespace.of(target.getPath().toArray(new String[0]));
        var resolvedTargetEntity = resolvedEntityView.getResolvedPath(namespace);
        if (resolvedTargetEntity == null) {
          throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        yield resolvedTargetEntity;
      }
      case TABLE_LIKE -> {
        var tableIdentifier = TableIdentifier.of(target.getPath().toArray(new String[0]));
        // only Iceberg tables are supported
        var resolvedTableEntity =
            resolvedEntityView.getResolvedPath(
                tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
        if (resolvedTableEntity == null) {
          throw new NoSuchTableException("Iceberg Table does not exist: %s", tableIdentifier);
        }
        yield resolvedTableEntity;
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }

  private static Policy toPolicy(PolarisBaseEntity polarisBaseEntity) {
    var policyEntity = PolicyEntity.of(polarisBaseEntity);
    return constructPolicy(policyEntity);
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
