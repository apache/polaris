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
package org.apache.polaris.service.catalog.iceberg;

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.CatalogHandlerRuntime;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.events.EventAttributeMap;

/** Immutable runtime configuration for {@link IcebergCatalogHandler}. */
@PolarisImmutable
public interface IcebergCatalogHandlerRuntime extends CatalogHandlerRuntime {

  static ImmutableIcebergCatalogHandlerRuntime.Builder builder() {
    return ImmutableIcebergCatalogHandlerRuntime.builder();
  }

  CatalogPrefixParser prefixParser();

  ResolverFactory resolverFactory();

  CallContextCatalogFactory catalogFactory();

  ReservedProperties reservedProperties();

  CatalogHandlerUtils catalogHandlerUtils();

  StorageAccessConfigProvider storageAccessConfigProvider();

  Instance<ExternalCatalogFactory> externalCatalogFactories();

  EventAttributeMap eventAttributeMap();
}
