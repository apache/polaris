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
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.junit.jupiter.api.Test;

/**
 * Pins the disabled-feature gate at the adapter's shared entry point (all tag routes funnel through
 * newHandler; listTags stands in for them): with ENABLE_TAG_STORE off, the adapter throws
 * UnsupportedOperationException before principal validation or any handler work.
 */
public class TagCatalogAdapterFeatureGateTest {

  @Test
  public void testDisabledFeatureRejectsBeforeAnyWork() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_TAG_STORE)).thenReturn(false);
    TagCatalogAdapter adapter =
        new TagCatalogAdapter(
            mock(CatalogPrefixParser.class), mock(TagCatalogHandlerFactory.class), realmConfig);

    // The gate fires before the security context or request body are touched.
    assertThatThrownBy(() -> adapter.listTags("cat", null, null, null, null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("ENABLE_TAG_STORE");
  }
}
