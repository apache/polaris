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
package org.apache.polaris.service.storage.azure;

import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.azure.AzureLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzureLocationTest {

  @Test
  public void testLocation() {
    String uri = "abfss://container@storageaccount.blob.core.windows.net/myfile";
    AzureLocation azureLocation = new AzureLocation(uri);
    Assertions.assertThat(azureLocation.getContainer()).isEqualTo("container");
    Assertions.assertThat(azureLocation.getStorageAccount()).isEqualTo("storageaccount");
    Assertions.assertThat(azureLocation.getEndpoint()).isEqualTo("blob.core.windows.net");
    Assertions.assertThat(azureLocation.getFilePath()).isEqualTo("myfile");
  }

  @Test
  public void testWasbLocation() {
    String uri = "wasb://container@storageaccount.blob.core.windows.net/myfile";
    AzureLocation azureLocation = new AzureLocation(uri);
    Assertions.assertThat(azureLocation.getContainer()).isEqualTo("container");
    Assertions.assertThat(azureLocation.getStorageAccount()).isEqualTo("storageaccount");
    Assertions.assertThat(azureLocation.getEndpoint()).isEqualTo("blob.core.windows.net");
    Assertions.assertThat(azureLocation.getFilePath()).isEqualTo("myfile");
  }

  @Test
  public void testCrossSchemeComparisons() {
    StorageLocation abfsLocation =
        AzureLocation.of("abfss://container@acc.dev.core.windows.net/some/file/x");
    StorageLocation wasbLocation =
        AzureLocation.of("wasb://container@acc.blob.core.windows.net/some/file");
    Assertions.assertThat(abfsLocation).isNotEqualTo(wasbLocation);
    Assertions.assertThat(abfsLocation.isChildOf(wasbLocation)).isTrue();
  }

  @Test
  public void testLocationComparisons() {
    StorageLocation location =
        AzureLocation.of("abfss://container-dash@acc.blob.core.windows.net/some_file/metadata");
    StorageLocation parentLocation =
        AzureLocation.of("abfss://container-dash@acc.blob.core.windows.net");
    StorageLocation parentLocationTrailingSlash =
        AzureLocation.of("abfss://container-dash@acc.blob.core.windows.net/");

    Assertions.assertThat(location).isNotEqualTo(parentLocation);
    Assertions.assertThat(location).isNotEqualTo(parentLocationTrailingSlash);

    Assertions.assertThat(location.isChildOf(parentLocation)).isTrue();
    Assertions.assertThat(location.isChildOf(parentLocationTrailingSlash)).isTrue();
  }

  @Test
  public void testLocation_negative_cases() {
    Assertions.assertThatThrownBy(
            () -> new AzureLocation("abfss://storageaccount.blob.core.windows.net/myfile"))
        .isInstanceOf(IllegalArgumentException.class);
    Assertions.assertThatThrownBy(
            () -> new AzureLocation("abfss://container@storageaccount/myfile"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
