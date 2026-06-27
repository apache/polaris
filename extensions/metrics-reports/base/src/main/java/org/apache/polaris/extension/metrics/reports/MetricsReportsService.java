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
package org.apache.polaris.extension.metrics.reports;

import com.google.common.annotations.Beta;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.service.metrics.api.PolarisCatalogsApiService;
import org.jspecify.annotations.NonNull;

/**
 * Service implementation for the Metrics Reports API.
 *
 * <p>Resolves catalog/namespace/table names to internal IDs and performs authorization. In this
 * release the read path returns HTTP 501 Not Implemented; durable query backing is provided by the
 * {@code polaris-extensions-metrics-reports-jdbc} extension in a follow-up release.
 *
 * <p>TODO (#4756): wire durable metrics query provider when the extension is installed.
 */
@Beta
@RequestScoped
public class MetricsReportsService implements PolarisCatalogsApiService {

  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;

  @Inject
  public MetricsReportsService(
      @NonNull PolarisAuthorizer authorizer,
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull ResolutionManifestFactory resolutionManifestFactory) {
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.resolutionManifestFactory = resolutionManifestFactory;
  }

  @Override
  public Response listTableMetrics(
      String catalogName,
      String namespace,
      String table,
      String metricType,
      String pageToken,
      Integer pageSize,
      Long snapshotId,
      String principalName,
      Long timestampFrom,
      Long timestampTo,
      RealmContext realmContext,
      SecurityContext securityContext) {

    Namespace ns = decodeNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(ns, table);

    resolveAndAuthorizeTableMetrics(catalogName, identifier);

    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            "Durable metrics query is not available in this deployment. "
                + "Install the polaris-extensions-metrics-reports-jdbc extension to enable it.")
        .build();
  }

  private void resolveAndAuthorizeTableMetrics(String catalogName, TableIdentifier identifier) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
    manifest.addPassthroughPath(
        new ResolverPath(
            Arrays.asList(identifier.namespace().levels()), PolarisEntityType.NAMESPACE));
    manifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE));
    ResolverStatus status = manifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "TopLevelEntity of type %s does not exist: %s",
          status.getFailedToResolvedEntityType(), status.getFailedToResolvedEntityName());
    }
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      throw new NotFoundException("Table not found: %s", identifier);
    }

    PolarisResolvedPathWrapper tableWrapper =
        manifest.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE, true);

    if (tableWrapper == null) {
      throw new NotFoundException("Table not found: %s", identifier);
    }

    authorizer.authorizeOrThrow(
        polarisPrincipal,
        manifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        PolarisAuthorizableOperation.LIST_TABLE_METRICS,
        tableWrapper,
        null);
  }

  private static Namespace decodeNamespace(String encodedNamespace) {
    if (encodedNamespace == null || encodedNamespace.isEmpty()) {
      throw new IllegalArgumentException("namespace must not be empty");
    }
    return NamespaceUtils.splitNamespace(
        encodedNamespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR);
  }
}
