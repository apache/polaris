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
package org.apache.polaris.service.catalog.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Instance.Handle;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;
import org.apache.polaris.service.idempotency.IdempotencyConfiguration;

@RequestScoped
public class CatalogConfigHandler {
  private static final List<String> ICEBERG_REST_CONFIG_DEFAULT_KEYS =
      List.of(RESTCatalogProperties.PAGE_SIZE, RESTCatalogProperties.METRICS_REPORTING_ENABLED);

  private final CatalogPrefixParser prefixParser;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisAuthorizer authorizer;
  private final Instance<CatalogConfigEndpointContributor> endpointContributors;
  private final IdempotencyConfiguration idempotencyConfiguration;
  private final RealmConfig realmConfig;

  @Inject
  public CatalogConfigHandler(
      CatalogPrefixParser prefixParser,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisAuthorizer authorizer,
      @Any Instance<CatalogConfigEndpointContributor> endpointContributors,
      IdempotencyConfiguration idempotencyConfiguration,
      RealmConfig realmConfig) {
    this.prefixParser = prefixParser;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.authorizer = authorizer;
    this.endpointContributors = endpointContributors;
    this.idempotencyConfiguration = idempotencyConfiguration;
    this.realmConfig = realmConfig;
  }

  public ConfigResponse getConfig(String catalogName, PolarisPrincipal principal) {
    PolarisResolutionManifest resolutionManifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalogName);
    resolutionManifest.addTopLevelName(
        catalogName, PolarisEntityType.CATALOG, false /* isOptional */);
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    AuthorizationRequest configAuthzRequest =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG_CONFIG,
                    PolarisSecurableMapper.catalog(catalogName))));
    authorizer.resolveAuthorizationInputs(authorizationState, configAuthzRequest);

    ResolverStatus resolverStatus = resolutionManifest.getPrimaryResolverStatusOrThrow();
    if (!resolverStatus.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
      throw new NotFoundException("Unable to find warehouse %s", catalogName);
    }
    PolarisBaseEntity catalogEntity =
        resolutionManifest
            .getResolvedTopLevelEntity(catalogName, PolarisEntityType.CATALOG)
            .getResolvedLeafEntity()
            .getEntity();

    // GET_CATALOG_CONFIG is opt-in via ENFORCE_CATALOG_CONFIG_AUTHORIZATION (default false).
    // When false, the endpoint is not authorization-gated so upgrades do not 403 bootstrap.
    // When true, CATALOG_READ_CONFIG is required for the whole endpoint. A 200 includes Iceberg
    // REST client keys in defaults when they are set on the catalog.
    boolean enforceAccessChecks =
        realmConfig.getConfig(FeatureConfiguration.ENFORCE_CATALOG_CONFIG_AUTHORIZATION);
    if (enforceAccessChecks) {
      authorizer.authorize(authorizationState, configAuthzRequest).throwIfDenied();
    }

    Map<String, String> catalogProperties = PolarisEntity.of(catalogEntity).getPropertiesAsMap();

    ConfigResponse.Builder builder =
        ConfigResponse.builder()
            .withDefaults(icebergRestConfigDefaults(catalogProperties))
            .withOverrides(
                ImmutableMap.of(
                    "prefix",
                    prefixParser.catalogNameToPrefix(catalogName),
                    // Polaris does not handle custom namespace separators;
                    // always communicate the default namespace separator to clients.
                    RESTCatalogProperties.NAMESPACE_SEPARATOR,
                    NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR_ENCODED))
            .withEndpoints(ImmutableList.copyOf(supportedEndpoints()));

    // Advertise Idempotency-Key support to clients. Per the REST spec, presence of this field
    // signals that mutation endpoints honor Idempotency-Key; its value is the reuse window a
    // client may retry a key within, which mirrors the server-side key TTL.
    if (idempotencyConfiguration.enabled()) {
      builder.withIdempotencyKeyLifetime(idempotencyConfiguration.ttl().toString());
    }

    return builder.build();
  }

  /**
   * Iceberg REST {@code GET /v1/config} defaults copy {@code rest-page-size} and {@code
   * rest-metrics-reporting-enabled} when present on the catalog. Other catalog properties remain on
   * Management GET catalog.
   */
  private static Map<String, String> icebergRestConfigDefaults(
      Map<String, String> catalogProperties) {
    ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
    for (String key : ICEBERG_REST_CONFIG_DEFAULT_KEYS) {
      String value = catalogProperties.get(key);
      if (value != null) {
        defaults.put(key, value);
      }
    }
    return defaults.build();
  }

  private Set<Endpoint> supportedEndpoints() {
    Set<Endpoint> endpoints = new LinkedHashSet<>();
    endpointContributors
        .handlesStream()
        .sorted(
            Comparator.comparingInt(CatalogConfigHandler::priority)
                .thenComparing(handle -> handle.getBean().getBeanClass().getName()))
        .map(Handle::get)
        .map(CatalogConfigEndpointContributor::endpoints)
        .forEach(endpoints::addAll);
    return endpoints;
  }

  private static int priority(Handle<CatalogConfigEndpointContributor> handle) {
    Priority priority = handle.getBean().getBeanClass().getAnnotation(Priority.class);
    return priority == null ? Integer.MAX_VALUE : priority.value();
  }
}
