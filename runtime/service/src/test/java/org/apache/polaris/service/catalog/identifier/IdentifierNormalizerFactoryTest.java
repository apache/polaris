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

import org.apache.polaris.core.entity.CatalogEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class IdentifierNormalizerFactoryTest {

  private final IdentifierNormalizerFactory factory = new IdentifierNormalizerFactory();

  @Test
  public void testGetNormalizerForCaseInsensitiveCatalog() {
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setCaseInsensitiveMode(true)
            .build();

    IdentifierNormalizer normalizer = factory.getNormalizer(catalog);

    Assertions.assertThat(normalizer).isInstanceOf(LowercaseNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing()).isTrue();
  }

  @Test
  public void testGetNormalizerForCaseSensitiveCatalog() {
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setCaseInsensitiveMode(false)
            .build();

    IdentifierNormalizer normalizer = factory.getNormalizer(catalog);

    Assertions.assertThat(normalizer).isInstanceOf(CasePreservingNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing()).isFalse();
  }

  @Test
  public void testGetNormalizerForCatalogWithoutCaseSetting() {
    CatalogEntity catalog = new CatalogEntity.Builder().setName("test-catalog").build();

    IdentifierNormalizer normalizer = factory.getNormalizer(catalog);

    Assertions.assertThat(normalizer).isInstanceOf(CasePreservingNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing()).isFalse();
  }

  @Test
  public void testGetNormalizerForNullCatalog() {
    IdentifierNormalizer normalizer = factory.getNormalizer(null);

    Assertions.assertThat(normalizer).isInstanceOf(CasePreservingNormalizer.class);
  }

  @Test
  public void testGetDefaultNormalizer() {
    IdentifierNormalizer normalizer = factory.getDefaultNormalizer();

    Assertions.assertThat(normalizer).isInstanceOf(CasePreservingNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing()).isFalse();
  }
}
