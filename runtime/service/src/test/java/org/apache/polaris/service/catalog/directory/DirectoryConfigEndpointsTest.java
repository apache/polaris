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
package org.apache.polaris.service.catalog.directory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.rest.DirectoryEndpoints;
import org.junit.jupiter.api.Test;

class DirectoryConfigEndpointsTest {

  private static RealmConfig realmConfig(boolean directories, boolean scan) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_DIRECTORIES)).thenReturn(directories);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_DIRECTORY_SCAN)).thenReturn(scan);
    return realmConfig;
  }

  @Test
  void directoriesDisabledListsNoEndpoint() {
    assertThat(DirectoryConfigEndpoints.getSupportedDirectoryEndpoints(realmConfig(false, true)))
        .isEmpty();
  }

  @Test
  void scanDisabledOmitsScanEndpoint() {
    assertThat(DirectoryConfigEndpoints.getSupportedDirectoryEndpoints(realmConfig(true, false)))
        .containsExactlyInAnyOrderElementsOf(DirectoryEndpoints.DIRECTORY_ENDPOINTS)
        .doesNotContain(DirectoryEndpoints.V1_SCAN_DIRECTORY);
  }

  @Test
  void scanEnabledListsScanEndpoint() {
    assertThat(DirectoryConfigEndpoints.getSupportedDirectoryEndpoints(realmConfig(true, true)))
        .contains(DirectoryEndpoints.V1_SCAN_DIRECTORY)
        .containsAll(DirectoryEndpoints.DIRECTORY_ENDPOINTS);
  }
}
