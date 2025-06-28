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
package org.apache.polaris.service.storage;

import java.net.URISyntaxException;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.azure.AzureLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageLocationTest {

  @Test
  public void testOfDifferentPrefixes() {
    StorageLocation standardLocation = StorageLocation.of("file:///path/to/file");
    StorageLocation slashLeadingLocation = StorageLocation.of("/path/to/file");
    StorageLocation manySlashLeadingLocation = StorageLocation.of("////////path/to/file");
    StorageLocation fileSingleSlashLocation = StorageLocation.of("file:/path/to/file");
    StorageLocation fileTooManySlashesLocation = StorageLocation.of("file://///////path/to/file");
    Assertions.assertThat(slashLeadingLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(manySlashLeadingLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(fileSingleSlashLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(fileTooManySlashesLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(standardLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(slashLeadingLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(manySlashLeadingLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(fileSingleSlashLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(fileTooManySlashesLocation).isExactlyInstanceOf(StorageLocation.class);
  }

  @Test
  public void testBlobStorageLocations() {
    StorageLocation azureLocation =
        StorageLocation.of("wasb://container@storageaccount.blob.core.windows.net/myfile");
    Assertions.assertThat(azureLocation instanceof AzureLocation).isTrue();
    azureLocation =
        StorageLocation.of("abfss://container@storageaccount.blob.core.windows.net/myfile");
    Assertions.assertThat(azureLocation instanceof AzureLocation).isTrue();

    String s3LocationStr = "s3://test-bucket/mydirectory";
    StorageLocation s3Location = StorageLocation.of(s3LocationStr);
    Assertions.assertThat(s3Location instanceof AzureLocation).isFalse();

    Assertions.assertThat(s3Location.toString()).isEqualTo(s3LocationStr);
  }

  @Test
  public void testSpecialCharacters() {
    // Blob Storage does not have validations
    String specialCharsBlobStorage = "s3://test-bucket/quote'/equals=/period/../myfile.parquet";
    StorageLocation s3LocationSpecialCharacters = StorageLocation.of(specialCharsBlobStorage);

    Assertions.assertThat(s3LocationSpecialCharacters.toString())
        .isEqualTo(specialCharsBlobStorage);

    // But local filesystems do
    String specialCharsLocalStorage = "file:///var/tmp\"/myfile.parquet";
    Assertions.assertThatThrownBy(
            () -> {
              StorageLocation.of(specialCharsLocalStorage);
            })
        .hasCauseInstanceOf(URISyntaxException.class);
  }
}
