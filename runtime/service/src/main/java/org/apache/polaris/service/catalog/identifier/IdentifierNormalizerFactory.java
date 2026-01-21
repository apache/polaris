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
package org.apache.polaris.service.catalog.identifier;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.core.entity.CatalogEntity;

/**
 * Factory for creating {@link IdentifierNormalizer} instances based on catalog configuration.
 *
 * <p>This is a CDI bean, allowing downstream implementations to override the normalization strategy
 * (e.g., uppercase instead of lowercase).
 */
@ApplicationScoped
public class IdentifierNormalizerFactory {

  /**
   * Get the appropriate normalizer for the given catalog.
   *
   * @param catalogEntity the catalog entity
   * @return the normalizer to use for this catalog
   */
  public IdentifierNormalizer getNormalizer(CatalogEntity catalogEntity) {
    if (catalogEntity != null && catalogEntity.isCaseInsensitive()) {
      return LowercaseNormalizer.INSTANCE;
    }
    return CasePreservingNormalizer.INSTANCE;
  }

  /**
   * Get the default case-preserving normalizer. Used when catalog entity is not yet resolved.
   *
   * @return the default normalizer
   */
  public IdentifierNormalizer getDefaultNormalizer() {
    return CasePreservingNormalizer.INSTANCE;
  }

  /**
   * Get the case-insensitive normalizer (lowercasing). Used when a catalog has case-insensitive
   * mode enabled.
   *
   * @return the case-insensitive normalizer
   */
  public IdentifierNormalizer getCaseInsensitiveNormalizer() {
    return LowercaseNormalizer.INSTANCE;
  }
}
