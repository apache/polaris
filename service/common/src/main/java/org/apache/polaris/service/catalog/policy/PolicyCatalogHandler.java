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
import java.util.HashSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.ListPoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
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
}
