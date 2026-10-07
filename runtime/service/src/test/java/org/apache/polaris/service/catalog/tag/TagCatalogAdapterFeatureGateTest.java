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
package org.apache.polaris.service.catalog.tag;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.junit.jupiter.api.Test;

/**
 * Pins the two availability checks at the adapter's shared entry point. All tag routes funnel
 * through newHandler, so listTags stands in for them: whether the realm has the feature switched
 * off or the configured metastore cannot store tags at all, the adapter refuses before principal
 * validation or any handler work.
 */
public class TagCatalogAdapterFeatureGateTest {

  @Test
  public void testDisabledFeatureRejectsBeforeAnyWork() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_TAG_STORE)).thenReturn(false);

    // The gate fires before the security context or request body are touched.
    assertThatThrownBy(
            () -> adapter(realmConfig, true).listTags("cat", null, null, null, null, null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("ENABLE_TAG_STORE");
  }

  @Test
  public void testMetastoreWithoutTagStorageRejectsBeforeAnyWork() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_TAG_STORE)).thenReturn(true);

    // The feature flag is a realm setting and cannot see which metastore is configured, so enabling
    // it on a metastore with no tag storage must still be refused here rather than deep in
    // persistence, where the failure would carry an internal entity-type code.
    assertThatThrownBy(
            () -> adapter(realmConfig, false).listTags("cat", null, null, null, null, null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not supported by the configured metastore");
  }

  private static TagCatalogAdapter adapter(RealmConfig realmConfig, boolean tagsSupported) {
    PolarisMetaStoreManager metaStoreManager = mock(PolarisMetaStoreManager.class);
    when(metaStoreManager.supportsEntityType(PolarisEntityType.TAG)).thenReturn(tagsSupported);
    return new TagCatalogAdapter(
        mock(CatalogPrefixParser.class),
        mock(TagCatalogHandlerFactory.class),
        realmConfig,
        metaStoreManager);
  }
}
