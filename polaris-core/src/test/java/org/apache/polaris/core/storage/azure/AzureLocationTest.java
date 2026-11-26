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
package org.apache.polaris.core.storage.azure;

import org.apache.polaris.core.storage.StorageLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class AzureLocationTest {

  @Test
  public void testLocation() {
    String uri = "abfss://container@storageaccount.blob.core.windows.net/myfile";
    AzureLocation azureLocation = new AzureLocation(uri);
    Assertions.assertThat(azureLocation.getContainer()).isEqualTo("container");
    Assertions.assertThat(azureLocation.getStorageAccount()).isEqualTo("storageaccount");
    Assertions.assertThat(azureLocation.getEndpoint()).isEqualTo("blob.core.windows.net");
    Assertions.assertThat(azureLocation.getStorageDnsName())
        .isEqualTo("storageaccount.blob.core.windows.net");
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

  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss", "wasb", "wasbs"})
  public void testCrossSchemeComparisons(String scheme) {
    StorageLocation abfsLocation =
        AzureLocation.of("abfss://container@acc.blob.core.windows.net/some/file/x");
    StorageLocation parentAnotherScheme =
        AzureLocation.of(scheme + "://container@acc.blob.core.windows.net/some/file");
    StorageLocation sameLocation =
        AzureLocation.of(scheme + "://container@acc.blob.core.windows.net/some/file/x");
    Assertions.assertThat(abfsLocation).isNotEqualTo(parentAnotherScheme);
    Assertions.assertThat(abfsLocation.isChildOf(parentAnotherScheme)).isTrue();
    Assertions.assertThat(abfsLocation).isEqualTo(sameLocation);
    Assertions.assertThat(abfsLocation.hashCode()).isEqualTo(sameLocation.hashCode());
    Assertions.assertThat(abfsLocation.isSameLocation(sameLocation)).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "blob.core.windows.net, dfs.core.windows.net",
    "blob.core.windows.net, blob.core.usgovcloudapi.net",
    "dfs.core.windows.net, dfs.core.chinacloudapi.cn",
    "blob.core.windows.net, custom.example.com"
  })
  public void testDifferentEndpointComparisons(String endpoint, String otherEndpoint) {
    StorageLocation location = AzureLocation.of("abfss://container@acc." + endpoint + "/some/file");
    StorageLocation otherLocation =
        AzureLocation.of("abfss://container@acc." + otherEndpoint + "/some/file");
    StorageLocation otherParent =
        AzureLocation.of("abfss://container@acc." + otherEndpoint + "/some");

    Assertions.assertThat(location).isNotEqualTo(otherLocation);
    Assertions.assertThat(otherLocation).isNotEqualTo(location);
    Assertions.assertThat(location.isSameLocation(otherLocation)).isFalse();
    Assertions.assertThat(location.isChildOf(otherLocation)).isFalse();
    Assertions.assertThat(otherLocation.isChildOf(location)).isFalse();
    Assertions.assertThat(location.isChildOf(otherParent)).isFalse();
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
  public void testLocationComparisonsWithEmptyPathSegment() {
    StorageLocation root = AzureLocation.of("abfss://container@acc.dfs.core.windows.net/");
    StorageLocation emptySegment = AzureLocation.of("abfss://container@acc.dfs.core.windows.net//");
    StorageLocation location =
        AzureLocation.of("abfss://container@acc.dfs.core.windows.net/some_file/metadata");
    StorageLocation locationUnderEmptySegment =
        AzureLocation.of("abfss://container@acc.dfs.core.windows.net//some_file/metadata");

    Assertions.assertThat(emptySegment.isChildOf(root)).isTrue();
    Assertions.assertThat(root.isChildOf(emptySegment)).isFalse();
    Assertions.assertThat(location.isChildOf(emptySegment)).isFalse();
    Assertions.assertThat(locationUnderEmptySegment.isChildOf(emptySegment)).isTrue();
    Assertions.assertThat(locationUnderEmptySegment.isChildOf(location)).isFalse();
    Assertions.assertThat(location.isChildOf(locationUnderEmptySegment)).isFalse();
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
