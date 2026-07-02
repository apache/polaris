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
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.catalog.semanticmodel.api.PolarisCatalogSemanticModelApiService;
import org.apache.polaris.service.catalog.semanticmodel.types.CreateSemanticModelRequest;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelIdentifier;
import org.apache.polaris.service.catalog.semanticmodel.types.UpdateSemanticModelRequest;

/**
 * Adapter for the OSI semantic-model API. The endpoints are gated by {@link
 * FeatureConfiguration#ENABLE_SEMANTIC_MODELS} and dispatch to {@link SemanticModelCatalogHandler}
 * for authorization, validation, source resolution, and persistence.
 */
@RequestScoped
public class SemanticModelCatalogAdapter
    implements PolarisCatalogSemanticModelApiService, CatalogAdapter {

  private final RealmConfig realmConfig;
  private final CatalogPrefixParser prefixParser;
  private final SemanticModelCatalogHandlerFactory handlerFactory;

  @Inject
  public SemanticModelCatalogAdapter(
      CallContext callContext,
      CatalogPrefixParser prefixParser,
      SemanticModelCatalogHandlerFactory handlerFactory) {
    this.realmConfig = callContext.getRealmConfig();
    this.prefixParser = prefixParser;
    this.handlerFactory = handlerFactory;
  }

  private SemanticModelCatalogHandler newHandler(SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_SEMANTIC_MODELS);
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    return handlerFactory.createHandler(catalogName, principal);
  }

  private static SemanticModelIdentifier identifier(Namespace namespace, String name) {
    return SemanticModelIdentifier.builder()
        .setNamespace(List.of(namespace.levels()))
        .setName(name)
        .build();
  }

  @Override
  public Response createSemanticModel(
      String prefix,
      String namespace,
      CreateSemanticModelRequest createSemanticModelRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns =
        NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
    SemanticModelCatalogHandler handler = newHandler(securityContext, prefix);
    return Response.ok(handler.createSemanticModel(ns, createSemanticModelRequest)).build();
  }

  @Override
  public Response listSemanticModels(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns =
        NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
    SemanticModelCatalogHandler handler = newHandler(securityContext, prefix);
    return Response.ok(handler.listSemanticModels(ns, pageToken, pageSize)).build();
  }

  @Override
  public Response loadSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns =
        NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
    SemanticModelCatalogHandler handler = newHandler(securityContext, prefix);
    return Response.ok(handler.loadSemanticModel(identifier(ns, semanticModelName))).build();
  }

  @Override
  public Response updateSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      UpdateSemanticModelRequest updateSemanticModelRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns =
        NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
    SemanticModelCatalogHandler handler = newHandler(securityContext, prefix);
    return Response.ok(
            handler.updateSemanticModel(
                identifier(ns, semanticModelName), updateSemanticModelRequest))
        .build();
  }

  @Override
  public Response dropSemanticModel(
      String prefix,
      String namespace,
      String semanticModelName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns =
        NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
    SemanticModelCatalogHandler handler = newHandler(securityContext, prefix);
    handler.dropSemanticModel(identifier(ns, semanticModelName));
    return Response.noContent().build();
  }
}
