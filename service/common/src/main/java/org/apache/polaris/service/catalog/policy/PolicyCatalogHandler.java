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

import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.ListPoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;

public class PolicyCatalogHandler extends CatalogHandler {

  private PolarisMetaStoreManager metaStoreManager;

  private PolicyCatalog policyCatalog;

  public PolicyCatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer) {
    super(callContext, entityManager, securityContext, catalogName, authorizer);
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  protected void initializeCatalog() {
    this.policyCatalog = new PolicyCatalog(metaStoreManager, callContext, this.resolutionManifest);
  }

  public ListPoliciesResponse listPolicies(Namespace parent, PolicyType policyType) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_POLICY;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return ListPoliciesResponse.builder()
        .setIdentifiers(new HashSet<>(policyCatalog.listPolicies(parent, policyType)))
        .build();
  }

  public LoadPolicyResponse createPolicy(Namespace namespace, CreatePolicyRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_POLICY;
    PolicyIdentifier identifier =
        PolicyIdentifier.builder().setNamespace(namespace).setName(request.getName()).build();

    // authorize the creating policy under namespace operation
    authorizeBasicNamespaceOperationOrThrow(
        op, identifier.getNamespace(), null, null, List.of(identifier));

    return LoadPolicyResponse.builder()
        .setPolicy(
            policyCatalog.createPolicy(
                identifier, request.getType(), request.getDescription(), request.getContent()))
        .build();
  }

  public LoadPolicyResponse loadPolicy(PolicyIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_POLICY;
    authorizeBasicPolicyOperationOrThrow(op, identifier);

    return LoadPolicyResponse.builder().setPolicy(policyCatalog.loadPolicy(identifier)).build();
  }

  public LoadPolicyResponse updatePolicy(PolicyIdentifier identifier, UpdatePolicyRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_POLICY;
    authorizeBasicPolicyOperationOrThrow(op, identifier);

    return LoadPolicyResponse.builder()
        .setPolicy(
            policyCatalog.updatePolicy(
                identifier,
                request.getDescription(),
                request.getContent(),
                request.getCurrentPolicyVersion()))
        .build();
  }

  public boolean dropPolicy(PolicyIdentifier identifier, boolean detachAll) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_POLICY;
    authorizeBasicPolicyOperationOrThrow(op, identifier);

    return policyCatalog.dropPolicy(identifier, detachAll);
  }

  public boolean attachPolicy(PolicyIdentifier identifier, AttachPolicyRequest request) {
    authorizePolicyAttachmentOperationOrThrow(identifier, request.getTarget(), true);
    return policyCatalog.attachPolicy(identifier, request.getTarget(), request.getParameters());
  }

  public boolean detachPolicy(PolicyIdentifier identifier, DetachPolicyRequest request) {
    authorizePolicyAttachmentOperationOrThrow(identifier, request.getTarget(), false);
    return policyCatalog.detachPolicy(identifier, request.getTarget());
  }

  private void authorizeBasicPolicyOperationOrThrow(
      PolarisAuthorizableOperation op, PolicyIdentifier identifier) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.identifierToList(identifier.getNamespace(), identifier.getName()),
            PolarisEntityType.POLICY,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();

    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(identifier, true);
    if (target == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
    }

    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  private void authorizePolicyAttachmentOperationOrThrow(
      PolicyIdentifier identifier, PolicyAttachmentTarget target, boolean isAttach) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.identifierToList(identifier.getNamespace(), identifier.getName()),
            PolarisEntityType.POLICY,
            true /* optional */),
        identifier);

    switch (target.getType()) {
      case CATALOG -> {}
      case NAMESPACE -> {
        Namespace targetNamespace = Namespace.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(Arrays.asList(targetNamespace.levels()), PolarisEntityType.NAMESPACE),
            targetNamespace);
      }
      case TABLE_LIKE -> {
        TableIdentifier targetIdentifier =
            TableIdentifier.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(targetIdentifier),
                PolarisEntityType.TABLE_LIKE),
            targetIdentifier);
      }
    }

    ResolverStatus status = resolutionManifest.resolveAll();

    throwNotFoundExceptionIfFailToResolve(status, identifier);

    PolarisResolvedPathWrapper policyWrapper =
        resolutionManifest.getPassthroughResolvedPath(
            identifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);
    if (policyWrapper == null) {
      throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
    }

    PolarisResolvedPathWrapper targetWrapper =
        PolicyCatalogUtils.getResolvedPathWrapper(resolutionManifest, target);

    PolarisAuthorizableOperation op =
        determinePolicyAttachmentOperation(target, targetWrapper, isAttach);

    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        policyWrapper,
        targetWrapper);

    initializeCatalog();
  }

  private PolarisAuthorizableOperation determinePolicyAttachmentOperation(
      PolicyAttachmentTarget target, PolarisResolvedPathWrapper targetWrapper, boolean isAttach) {
    return switch (targetWrapper.getRawLeafEntity().getType()) {
      case CATALOG ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG;
      case NAMESPACE ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE;
      case TABLE_LIKE -> {
        PolarisEntitySubType subType = targetWrapper.getRawLeafEntity().getSubType();
        if (subType == PolarisEntitySubType.ICEBERG_TABLE) {
          yield isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE;
        }
        throw new IllegalArgumentException("Unsupported table-like subtype: " + subType);
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }

  private void throwNotFoundExceptionIfFailToResolve(
      ResolverStatus status, PolicyIdentifier identifier) {
    if ((status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED)) {
      switch (status.getFailedToResolvePath().getLastEntityType()) {
        case PolarisEntityType.TABLE_LIKE ->
            throw new NoSuchTableException(
                "Table or view does not exist: %s",
                PolarisCatalogHelpers.listToTableIdentifier(
                    status.getFailedToResolvePath().getEntityNames()));
        case PolarisEntityType.NAMESPACE ->
            throw new NoSuchNamespaceException(
                "Namespace does not exist: %s",
                Namespace.of(
                    status.getFailedToResolvePath().getEntityNames().toArray(new String[0])));
        case PolarisEntityType.POLICY ->
            throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
        default -> throw new IllegalStateException("Cannot resolve");
      }
    }
  }
}
