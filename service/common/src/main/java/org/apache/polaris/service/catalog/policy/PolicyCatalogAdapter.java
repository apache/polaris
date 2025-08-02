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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogPolicyApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.ListPoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class PolicyCatalogAdapter implements PolarisCatalogPolicyApiService, CatalogAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolicyCatalogAdapter.class);

  private final RealmContext realmContext;
  private final CallContext callContext;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisAuthorizer polarisAuthorizer;
  private final CatalogPrefixParser prefixParser;

  @Inject
  public PolicyCatalogAdapter(
      RealmContext realmContext,
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      PolarisAuthorizer polarisAuthorizer,
      CatalogPrefixParser prefixParser) {
    this.realmContext = realmContext;
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.polarisAuthorizer = polarisAuthorizer;
    this.prefixParser = prefixParser;
  }

  private PolicyCatalogHandler newHandlerWrapper(SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        callContext, FeatureConfiguration.ENABLE_POLICY_STORE);
    validatePrincipal(securityContext);

    return new PolicyCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext,
        prefixParser.prefixToCatalogName(realmContext, prefix),
        polarisAuthorizer);
  }

  @Override
  public Response createPolicy(
      String prefix,
      String namespace,
      CreatePolicyRequest createPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadPolicyResponse response = handler.createPolicy(ns, createPolicyRequest);
    return Response.ok(response).build();
  }

  @Override
  public Response listPolicies(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      String policyType,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyType type =
        policyType != null ? PolicyType.fromName(RESTUtil.decodeString(policyType)) : null;
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    ListPoliciesResponse response = handler.listPolicies(ns, type);
    return Response.ok(response).build();
  }

  @Override
  public Response loadPolicy(
      String prefix,
      String namespace,
      String policyName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyIdentifier identifier = new PolicyIdentifier(ns, RESTUtil.decodeString(policyName));
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadPolicyResponse response = handler.loadPolicy(identifier);
    return Response.ok(response).build();
  }

  @Override
  public Response updatePolicy(
      String prefix,
      String namespace,
      String policyName,
      UpdatePolicyRequest updatePolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyIdentifier identifier = new PolicyIdentifier(ns, RESTUtil.decodeString(policyName));
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadPolicyResponse response = handler.updatePolicy(identifier, updatePolicyRequest);
    return Response.ok(response).build();
  }

  @Override
  public Response dropPolicy(
      String prefix,
      String namespace,
      String policyName,
      Boolean detachAll,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyIdentifier identifier = new PolicyIdentifier(ns, RESTUtil.decodeString(policyName));
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    handler.dropPolicy(identifier, detachAll != null && detachAll);
    return Response.noContent().build();
  }

  @Override
  public Response attachPolicy(
      String prefix,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyIdentifier identifier = new PolicyIdentifier(ns, RESTUtil.decodeString(policyName));
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    handler.attachPolicy(identifier, attachPolicyRequest);
    return Response.noContent().build();
  }

  @Override
  public Response detachPolicy(
      String prefix,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    PolicyIdentifier identifier = new PolicyIdentifier(ns, RESTUtil.decodeString(policyName));
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    handler.detachPolicy(identifier, detachPolicyRequest);
    return Response.noContent().build();
  }

  @Override
  public Response getApplicablePolicies(
      String prefix,
      String pageToken,
      Integer pageSize,
      String namespace,
      String targetName,
      String policyType,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = namespace != null ? decodeNamespace(namespace) : null;
    String target = targetName != null ? RESTUtil.decodeString(targetName) : null;
    PolicyType type =
        policyType != null ? PolicyType.fromName(RESTUtil.decodeString(policyType)) : null;
    PolicyCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    GetApplicablePoliciesResponse response = handler.getApplicablePolicies(ns, target, type);
    return Response.ok(response).build();
  }
}
