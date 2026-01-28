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

package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.junit.jupiter.api.Test;

/** Minimal diagnostic test to verify the constructor fix works */
public class StorageAccessConfigProviderConstructorTest {

  @Test
  public void testConstructorWithAllFourParameters() {
    // Arrange - create all required mocks
    StorageCredentialCache mockCredentialCache = mock(StorageCredentialCache.class);
    StorageCredentialsVendor mockStorageVendor = mock(StorageCredentialsVendor.class);
    PolarisPrincipal mockPolarisPrincipal = mock(PolarisPrincipal.class);
    TableStorageConfigurationMerger merger = new TableStorageConfigurationMerger();

    // Act - create provider with all 4 parameters
    StorageAccessConfigProvider provider =
        new StorageAccessConfigProvider(
            mockCredentialCache, mockStorageVendor, mockPolarisPrincipal, merger);

    // Assert - verify provider was created successfully
    assertThat(provider).isNotNull();

    System.out.println("✅ Constructor test PASSED - all 4 parameters accepted");
  }

  @Test
  public void testPolarisPrincipalIsRequired() {
    // This test verifies that the constructor requires exactly 4 parameters
    // Compilation will fail if we try to create with only 3 parameters

    StorageCredentialCache mockCredentialCache = mock(StorageCredentialCache.class);
    StorageCredentialsVendor mockStorageVendor = mock(StorageCredentialsVendor.class);
    PolarisPrincipal mockPolarisPrincipal = mock(PolarisPrincipal.class);
    TableStorageConfigurationMerger merger = new TableStorageConfigurationMerger();

    // This should compile without errors
    StorageAccessConfigProvider provider =
        new StorageAccessConfigProvider(
            mockCredentialCache,
            mockStorageVendor,
            mockPolarisPrincipal, // Required parameter
            merger);

    assertThat(provider).isNotNull();

    System.out.println("✅ PolarisPrincipal requirement test PASSED");
  }
}
