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

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.semanticmodel.types.CreateSemanticModelRequest;
import org.apache.polaris.service.catalog.semanticmodel.types.ListSemanticModelsResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.LoadSemanticModelResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelIdentifier;
import org.apache.polaris.service.catalog.semanticmodel.types.UpdateSemanticModelRequest;

/**
 * Authorizes and delegates OSI semantic-model operations to {@link SemanticModelCatalog}. Mirrors
 * {@link org.apache.polaris.service.catalog.policy.PolicyCatalogHandler}.
 *
 * <p>Authorization is intentionally minimal in this phase: operations are gated by the coarse
 * {@code CATALOG_MANAGE_CONTENT} privilege (see {@code RbacOperationSemantics}). The dedicated
 * {@code SEMANTIC_MODEL_*} privilege matrix, the write-time source-access check, and the
 * independent/propagated read-time enforcement modes land in the authorization phase.
 */
@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class SemanticModelCatalogHandler extends CatalogHandler {

  private SemanticModelCatalog semanticModelCatalog;

  @Override
  protected void initializeCatalog() {
    this.semanticModelCatalog =
        new SemanticModelCatalog(
            metaStoreManager(),
            callContext(),
            this.resolutionManifest,
            resolutionManifestFactory(),
            polarisPrincipal());
  }

  public LoadSemanticModelResponse createSemanticModel(
      Namespace namespace, CreateSemanticModelRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_SEMANTIC_MODEL;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    SemanticModelIdentifier identifier =
        SemanticModelIdentifier.builder()
            .setNamespace(List.of(namespace.levels()))
            .setName(request.getName())
            .build();
    return semanticModelCatalog.createSemanticModel(identifier, request.getDocument());
  }

  public ListSemanticModelsResponse listSemanticModels(
      Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_SEMANTIC_MODEL;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
    return semanticModelCatalog.listSemanticModels(namespace, pageRequest);
  }

  public LoadSemanticModelResponse loadSemanticModel(SemanticModelIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_SEMANTIC_MODEL;
    authorizeBasicSemanticModelOperationOrThrow(op, identifier);
    return semanticModelCatalog.loadSemanticModel(identifier);
  }

  public LoadSemanticModelResponse updateSemanticModel(
      SemanticModelIdentifier identifier, UpdateSemanticModelRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_SEMANTIC_MODEL;
    authorizeBasicSemanticModelOperationOrThrow(op, identifier);
    return semanticModelCatalog.updateSemanticModel(
        identifier, request.getDocument(), request.getEntityVersion());
  }

  public void dropSemanticModel(SemanticModelIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_SEMANTIC_MODEL;
    authorizeBasicSemanticModelOperationOrThrow(op, identifier);
    semanticModelCatalog.dropSemanticModel(identifier);
  }

  private void authorizeBasicSemanticModelOperationOrThrow(
      PolarisAuthorizableOperation op, SemanticModelIdentifier identifier) {
    Namespace namespace = Namespace.of(identifier.getNamespace().toArray(new String[0]));
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.identifierToList(namespace, identifier.getName()),
            PolarisEntityType.SEMANTIC_MODEL,
            true /* optional */));
    resolutionManifest.resolveAll();

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.ofSemanticModel(namespace, identifier.getName()), true);
    if (target == null) {
      throw new NoSuchSemanticModelException(
          String.format("Semantic model does not exist: %s", identifier.getName()));
    }

    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }
}
