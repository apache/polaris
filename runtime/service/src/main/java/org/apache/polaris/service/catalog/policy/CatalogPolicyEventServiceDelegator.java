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
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogPolicyApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.CatalogPolicyServiceEvents;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.UpdatePolicyRequest;

@Decorator
@Priority(1000)
public class CatalogPolicyEventServiceDelegator
    implements PolarisCatalogPolicyApiService, CatalogAdapter {

  @Inject @Delegate PolicyCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createPolicy(
      String prefix,
      String namespace,
      CreatePolicyRequest createPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeCreatePolicy(
        new CatalogPolicyServiceEvents.BeforeCreatePolicyEvent(
            eventMetadataFactory.create(), catalogName, namespace, createPolicyRequest));
    Response resp =
        delegate.createPolicy(
            prefix, namespace, createPolicyRequest, realmContext, securityContext);
    polarisEventListener.onAfterCreatePolicy(
        new CatalogPolicyServiceEvents.AfterCreatePolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            (LoadPolicyResponse) resp.getEntity()));
    return resp;
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
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeListPolicies(
        new CatalogPolicyServiceEvents.BeforeListPoliciesEvent(
            eventMetadataFactory.create(), catalogName, namespace, policyType));
    Response resp =
        delegate.listPolicies(
            prefix, namespace, pageToken, pageSize, policyType, realmContext, securityContext);
    polarisEventListener.onAfterListPolicies(
        new CatalogPolicyServiceEvents.AfterListPoliciesEvent(
            eventMetadataFactory.create(), catalogName, namespace, policyType));
    return resp;
  }

  @Override
  public Response loadPolicy(
      String prefix,
      String namespace,
      String policyName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeLoadPolicy(
        new CatalogPolicyServiceEvents.BeforeLoadPolicyEvent(
            eventMetadataFactory.create(), catalogName, namespace, policyName));
    Response resp =
        delegate.loadPolicy(prefix, namespace, policyName, realmContext, securityContext);
    polarisEventListener.onAfterLoadPolicy(
        new CatalogPolicyServiceEvents.AfterLoadPolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            (LoadPolicyResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response updatePolicy(
      String prefix,
      String namespace,
      String policyName,
      UpdatePolicyRequest updatePolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeUpdatePolicy(
        new CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            policyName,
            updatePolicyRequest));
    Response resp =
        delegate.updatePolicy(
            prefix, namespace, policyName, updatePolicyRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdatePolicy(
        new CatalogPolicyServiceEvents.AfterUpdatePolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            (LoadPolicyResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response dropPolicy(
      String prefix,
      String namespace,
      String policyName,
      Boolean detachAll,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeDropPolicy(
        new CatalogPolicyServiceEvents.BeforeDropPolicyEvent(
            eventMetadataFactory.create(), catalogName, namespace, policyName, detachAll));
    Response resp =
        delegate.dropPolicy(
            prefix, namespace, policyName, detachAll, realmContext, securityContext);
    polarisEventListener.onAfterDropPolicy(
        new CatalogPolicyServiceEvents.AfterDropPolicyEvent(
            eventMetadataFactory.create(), catalogName, namespace, policyName, detachAll));
    return resp;
  }

  @Override
  public Response attachPolicy(
      String prefix,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeAttachPolicy(
        new CatalogPolicyServiceEvents.BeforeAttachPolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            policyName,
            attachPolicyRequest));
    Response resp =
        delegate.attachPolicy(
            prefix, namespace, policyName, attachPolicyRequest, realmContext, securityContext);
    polarisEventListener.onAfterAttachPolicy(
        new CatalogPolicyServiceEvents.AfterAttachPolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            policyName,
            attachPolicyRequest));
    return resp;
  }

  @Override
  public Response detachPolicy(
      String prefix,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeDetachPolicy(
        new CatalogPolicyServiceEvents.BeforeDetachPolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            policyName,
            detachPolicyRequest));
    Response resp =
        delegate.detachPolicy(
            prefix, namespace, policyName, detachPolicyRequest, realmContext, securityContext);
    polarisEventListener.onAfterDetachPolicy(
        new CatalogPolicyServiceEvents.AfterDetachPolicyEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            policyName,
            detachPolicyRequest));
    return resp;
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
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeGetApplicablePolicies(
        new CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent(
            eventMetadataFactory.create(), catalogName, namespace, targetName, policyType));
    Response resp =
        delegate.getApplicablePolicies(
            prefix,
            pageToken,
            pageSize,
            namespace,
            targetName,
            policyType,
            realmContext,
            securityContext);
    polarisEventListener.onAfterGetApplicablePolicies(
        new CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent(
            eventMetadataFactory.create(),
            catalogName,
            namespace,
            targetName,
            policyType,
            (GetApplicablePoliciesResponse) resp.getEntity()));
    return resp;
  }
}
