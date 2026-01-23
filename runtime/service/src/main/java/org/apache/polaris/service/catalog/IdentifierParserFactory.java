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
package org.apache.polaris.service.catalog;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizerFactory;

@ApplicationScoped
public class IdentifierParserFactory {

  private final IdentifierNormalizerFactory normalizerFactory;
  private final ResolutionManifestFactory resolutionManifestFactory;

  @Inject
  public IdentifierParserFactory(
      IdentifierNormalizerFactory normalizerFactory,
      ResolutionManifestFactory resolutionManifestFactory) {
    this.normalizerFactory = normalizerFactory;
    this.resolutionManifestFactory = resolutionManifestFactory;
  }

  public IdentifierParser createParser(PolarisPrincipal principal, String catalogName) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalogName);
    manifest.resolveAll();
    return createParser(manifest);
  }

  public IdentifierParser createParser(PolarisResolutionManifest manifest) {
    CatalogEntity catalog = manifest.getResolvedCatalogEntity();
    boolean isCaseInsensitive = catalog != null && catalog.isCaseInsensitive();
    IdentifierNormalizer normalizer = getNormalizer(isCaseInsensitive);
    return new IdentifierParser(normalizer);
  }

  public boolean isCaseInsensitive(PolarisResolutionManifest manifest) {
    CatalogEntity catalog = manifest.getResolvedCatalogEntity();
    return catalog != null && catalog.isCaseInsensitive();
  }

  private IdentifierNormalizer getNormalizer(boolean isCaseInsensitive) {
    if (isCaseInsensitive) {
      return normalizerFactory.getCaseInsensitiveNormalizer();
    }
    return normalizerFactory.getDefaultNormalizer();
  }
}
