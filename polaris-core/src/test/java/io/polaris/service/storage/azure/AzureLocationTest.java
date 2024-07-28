/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.storage.azure;

import io.polaris.core.storage.azure.AzureLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzureLocationTest {

  @Test
  public void testLocation() {
    String uri = "abfss://container@storageaccount.blob.core.windows.net/myfile";
    AzureLocation azureLocation = new AzureLocation(uri);
    Assertions.assertEquals("container", azureLocation.getContainer());
    Assertions.assertEquals("storageaccount", azureLocation.getStorageAccount());
    Assertions.assertEquals("blob.core.windows.net", azureLocation.getEndpoint());
    Assertions.assertEquals("myfile", azureLocation.getFilePath());
  }

  @Test
  public void testLocation_negative_cases() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("wasbs://container@storageaccount.blob.core.windows.net/myfile"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("abfss://storageaccount.blob.core.windows.net/myfile"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("abfss://container@storageaccount/myfile"));
  }
}
