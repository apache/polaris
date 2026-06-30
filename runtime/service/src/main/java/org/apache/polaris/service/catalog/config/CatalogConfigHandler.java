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
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.CatalogConfigEndpointContributor;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.service.catalog.CatalogPrefixParser;

@RequestScoped
public class CatalogConfigHandler {
  private final CatalogPrefixParser prefixParser;
  private final ResolverFactory resolverFactory;
  private final Instance<CatalogConfigEndpointContributor> endpointContributors;

  @Inject
  public CatalogConfigHandler(
      CatalogPrefixParser prefixParser,
      ResolverFactory resolverFactory,
      @Any Instance<CatalogConfigEndpointContributor> endpointContributors) {
    this.prefixParser = prefixParser;
    this.resolverFactory = resolverFactory;
    this.endpointContributors = endpointContributors;
  }

  public ConfigResponse getConfig(String catalogName, PolarisPrincipal principal) {
    Resolver resolver = resolverFactory.createResolver(principal, catalogName);
    ResolverStatus resolverStatus = resolver.resolveAll();
    if (!resolverStatus.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
      throw new NotFoundException("Unable to find warehouse %s", catalogName);
    }
    ResolvedPolarisEntity resolvedReferenceCatalog = resolver.getResolvedReferenceCatalog();
    Map<String, String> properties =
        PolarisEntity.of(resolvedReferenceCatalog.getEntity()).getPropertiesAsMap();

    return ConfigResponse.builder()
        .withDefaults(properties)
        .withOverrides(
            ImmutableMap.of(
                "prefix",
                prefixParser.catalogNameToPrefix(catalogName),
                // Polaris does not handle custom namespace separators;
                // always communicate the default namespace separator to clients.
                RESTCatalogProperties.NAMESPACE_SEPARATOR,
                NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR_ENCODED))
        .withEndpoints(ImmutableList.copyOf(supportedEndpoints()))
        .build();
  }

  private Set<Endpoint> supportedEndpoints() {
    Set<Endpoint> endpoints = new LinkedHashSet<>();
    endpointContributors.stream()
        .map(CatalogConfigEndpointContributor::endpoints)
        .forEach(endpoints::addAll);
    return endpoints;
  }
}
