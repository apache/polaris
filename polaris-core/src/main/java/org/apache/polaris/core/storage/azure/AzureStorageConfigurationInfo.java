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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.Objects;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;

/** Azure storage configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableAzureStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAzureStorageConfigurationInfo.class)
@JsonTypeName("AzureStorageConfigurationInfo")
public abstract class AzureStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableAzureStorageConfigurationInfo.Builder builder() {
    return ImmutableAzureStorageConfigurationInfo.builder();
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.AZURE;
  }

  /** Azure tenant ID. */
  public abstract String getTenantId();

  /** The multi tenant app name for the service principal */
  @Nullable
  public abstract String getMultiTenantAppName();

  /** The consent url to the Azure permissions request page */
  @Nullable
  public abstract String getConsentUrl();

  @Override
  public void validatePrefixForStorageType(String loc) {
    AzureLocation location = new AzureLocation(loc);
    Objects.requireNonNull(
        location); // do something with the variable so the JVM doesn't optimize out the check
  }
}
