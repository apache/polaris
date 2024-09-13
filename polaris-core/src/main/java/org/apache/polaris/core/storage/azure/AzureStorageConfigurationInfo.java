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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/** Azure storage configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableAzureStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAzureStorageConfigurationInfo.class)
public abstract class AzureStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // technically there is no limitation since expectation for Azure locations are for the same
  // storage account and same container
  private static final int MAX_ALLOWED_LOCATIONS = 20;

  public static AzureStorageConfigurationInfo of(
      Iterable<String> allowedLocations, String tenantId) {
    return ImmutableAzureStorageConfigurationInfo.builder()
        .allowedLocations(allowedLocations)
        .tenantId(tenantId)
        .build();
  }

  @Override
  public abstract List<String> getAllowedLocations();

  @Override
  public StorageType getStorageType() {
    return StorageType.AZURE;
  }

  @Value.Default
  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
  }

  public abstract String getTenantId();

  /** The multi tenant app name for the service principal */
  @Nullable
  public abstract String getMultiTenantAppName();

  /** The consent url to the Azure permissions request page */
  @Nullable
  public abstract String getConsentUrl();

  @Override
  protected OptionalInt getMaxAllowedLocations() {
    return OptionalInt.of(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  protected void validatePrefixForStorageType(String loc) {
    AzureLocation location = new AzureLocation(loc);
    Objects.requireNonNull(
        location); // do something with the variable so the JVM doesn't optimize out the check
  }
}
