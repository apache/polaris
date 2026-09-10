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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class CatalogConfigHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogConfigHandler.class);
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
    // Separate AuthorizationRequests: GET_CATALOG_CONFIG is mandatory when enforcement is on,
    // while GET_CATALOG_CONFIG_PROPERTIES is best-effort. They must not share an AND-combined
    // multi-intent request (PolarisAuthorizer.authorize ANDs intents within one request).
    AuthorizationRequest configAuthzRequest =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG_CONFIG,
                    PolarisSecurableMapper.catalog(catalogName))));
    AuthorizationRequest propertiesAuthzRequest =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG_CONFIG_PROPERTIES,
                    PolarisSecurableMapper.catalog(catalogName))));
    // Resolve once (PolarisAuthorizerImpl.resolveAuthorizationInputs always resolveAll()s the
    // manifest; a second call would fail). Authorize uses separate requests below.
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

    // Both the endpoint hard-gate and the properties soft-hide are opt-in via
    // ENFORCE_CATALOG_CONFIG_AUTHORIZATION (default false). When false, behavior matches the
    // pre-authorization path so upgrades do not empty defaults or 403 bootstrap. When true:
    // GET_CATALOG_CONFIG (CATALOG_READ_CONFIG) denies the whole response if unauthorized;
    // GET_CATALOG_CONFIG_PROPERTIES (CATALOG_READ_PROPERTIES) soft-hides defaults.
    boolean enforce =
        realmConfig.getConfig(FeatureConfiguration.ENFORCE_CATALOG_CONFIG_AUTHORIZATION);
    if (enforce) {
      authorizer.authorize(authorizationState, configAuthzRequest).throwIfDenied();
    }

    ConfigResponse.Builder builder =
        ConfigResponse.builder()
            .withOverrides(
                ImmutableMap.of(
                    "prefix",
                    prefixParser.catalogNameToPrefix(catalogName),
                    // Polaris does not handle custom namespace separators;
                    // always communicate the default namespace separator to clients.
                    RESTCatalogProperties.NAMESPACE_SEPARATOR,
                    NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR_ENCODED))
            .withEndpoints(ImmutableList.copyOf(supportedEndpoints()));
    if (enforce) {
      if (authorizer.authorize(authorizationState, propertiesAuthzRequest).isAllowed()) {
        builder.withDefaults(PolarisEntity.of(catalogEntity).getPropertiesAsMap());
      } else {
        LOGGER.debug(
            "Principal '{}' may read catalog config but not catalog properties of '{}'",
            principal.getName(),
            catalogName);
      }
    } else {
      builder.withDefaults(PolarisEntity.of(catalogEntity).getPropertiesAsMap());
    }

    // Advertise Idempotency-Key support to clients. Per the REST spec, presence of this field
    // signals that mutation endpoints honor Idempotency-Key; its value is the reuse window a
    // client may retry a key within, which mirrors the server-side key TTL.
    if (idempotencyConfiguration.enabled()) {
      builder.withIdempotencyKeyLifetime(idempotencyConfiguration.ttl().toString());
    }

    return builder.build();
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
