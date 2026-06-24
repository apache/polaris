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
package org.apache.polaris.service.catalog.semanticmodel;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.PolarisCatalogSemanticModelApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.CreateSemanticModelRequest;
import org.apache.polaris.service.types.UpdateSemanticModelRequest;

/**
 * Stub adapter for the OSI semantic-model API. The endpoints are wired and gated by {@link
 * FeatureConfiguration#ENABLE_SEMANTIC_MODELS}, but every operation returns {@code 501 Not
 * Implemented}. Persistence, validation, authorization, and source-link resolution land in
 * subsequent phases.
 */
@RequestScoped
public class SemanticModelCatalogAdapter
    implements PolarisCatalogSemanticModelApiService, CatalogAdapter {

  private final RealmConfig realmConfig;

  @Inject
  public SemanticModelCatalogAdapter(CallContext callContext) {
    this.realmConfig = callContext.getRealmConfig();
  }

  private void ensureEnabled() {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_SEMANTIC_MODELS);
  }

  private static Response notImplemented() {
    return Response.status(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response createSemanticModel(
      String prefix,
      String namespace,
      CreateSemanticModelRequest createSemanticModelRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ensureEnabled();
    // TODO: authorize the principal, validate the OSI document against the bundled OSI JSON Schema,
    //  then persist a new semantic-model entity (entity version 0) and return the stored document
    //  with its entity-version. Tracked for a follow-up phase; returns 501 until persistence lands.
    return notImplemented();
  }

  @Override
  public Response listSemanticModels(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ensureEnabled();
    // TODO: authorize the principal, then page through the namespace's semantic-model entities and
    //  return their identifiers. Tracked for a follow-up phase; returns 501 until persistence lands.
    return notImplemented();
  }

  @Override
  public Response loadSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ensureEnabled();
    // TODO: authorize the principal, then read and return the stored OSI document and its
    //  entity-version. Tracked for a follow-up phase; returns 501 until persistence lands.
    return notImplemented();
  }

  @Override
  public Response updateSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      UpdateSemanticModelRequest updateSemanticModelRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ensureEnabled();
    // TODO: authorize the principal, validate the OSI document, then replace the stored document.
    //  When current-version is supplied, enforce optimistic concurrency (CAS) and fail with 409 on
    //  mismatch; otherwise last-writer-wins. Increment the entity version and return the stored
    //  document with its entity-version. Tracked for a follow-up phase; returns 501 until
    //  persistence lands.
    return notImplemented();
  }

  @Override
  public Response dropSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ensureEnabled();
    // TODO: authorize the principal, then delete the semantic-model entity. Tracked for a follow-up
    //  phase; returns 501 until persistence lands.
    return notImplemented();
  }
}
