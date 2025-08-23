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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.ApiBaseImplementation;
import org.apache.polaris.service.catalog.api.PolarisCatalogPolicyApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.UpdatePolicyRequest;

@RequestScoped
@Alternative
@Priority(1000) // Will allow downstream project-specific delegators to be added and used
public class PolicyCatalogAdapterDefaultDelegator
    implements PolarisCatalogPolicyApiService, CatalogAdapter {
  private final PolicyCatalogAdapter delegate;

  @Inject
  public PolicyCatalogAdapterDefaultDelegator(
      @ApiBaseImplementation PolicyCatalogAdapter delegate) {
    this.delegate = delegate;
  }

  @Override
  public Response createPolicy(
      String prefix,
      String namespace,
      CreatePolicyRequest createPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.createPolicy(
        prefix, namespace, createPolicyRequest, realmContext, securityContext);
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
    return delegate.listPolicies(
        prefix, namespace, pageToken, pageSize, policyType, realmContext, securityContext);
  }

  @Override
  public Response loadPolicy(
      String prefix,
      String namespace,
      String policyName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.loadPolicy(prefix, namespace, policyName, realmContext, securityContext);
  }

  @Override
  public Response updatePolicy(
      String prefix,
      String namespace,
      String policyName,
      UpdatePolicyRequest updatePolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.updatePolicy(
        prefix, namespace, policyName, updatePolicyRequest, realmContext, securityContext);
  }

  @Override
  public Response dropPolicy(
      String prefix,
      String namespace,
      String policyName,
      Boolean detachAll,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.dropPolicy(
        prefix, namespace, policyName, detachAll, realmContext, securityContext);
  }

  @Override
  public Response attachPolicy(
      String prefix,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.attachPolicy(
        prefix, namespace, policyName, attachPolicyRequest, realmContext, securityContext);
  }

  @Override
  public Response detachPolicy(
      String prefix,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.detachPolicy(
        prefix, namespace, policyName, detachPolicyRequest, realmContext, securityContext);
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
    return delegate.getApplicablePolicies(
        prefix,
        pageToken,
        pageSize,
        namespace,
        targetName,
        policyType,
        realmContext,
        securityContext);
  }
}
