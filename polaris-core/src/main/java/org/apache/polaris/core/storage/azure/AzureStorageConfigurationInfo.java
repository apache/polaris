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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/** Azure storage configuration information. */
public class AzureStorageConfigurationInfo extends PolarisStorageConfigurationInfo {
  // technically there is no limitation since expectation for Azure locations are for the same
  // storage account and same container
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 20;

  // Azure tenant id
  private final @Nonnull String tenantId;

  /** The multi tenant app name for the service principal */
  @JsonProperty(value = "multiTenantAppName", required = false)
  private @Nullable String multiTenantAppName = null;

  /** The consent url to the Azure permissions request page */
  @JsonProperty(value = "consentUrl", required = false)
  private @Nullable String consentUrl = null;

  @JsonCreator
  public AzureStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @Nonnull
          List<String> allowedLocations,
      @JsonProperty(value = "tenantId", required = true) @Nonnull String tenantId) {
    super(StorageType.AZURE, allowedLocations);
    this.tenantId = tenantId;
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
  }

  public @Nonnull String getTenantId() {
    return tenantId;
  }

  public @Nullable String getMultiTenantAppName() {
    return multiTenantAppName;
  }

  public void setMultiTenantAppName(@Nullable String multiTenantAppName) {
    this.multiTenantAppName = multiTenantAppName;
  }

  public @Nullable String getConsentUrl() {
    return consentUrl;
  }

  public void setConsentUrl(@Nullable String consentUrl) {
    this.consentUrl = consentUrl;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", getStorageType())
        .add("tenantId", tenantId)
        .add("allowedLocation", getAllowedLocations())
        .add("multiTenantAppName", multiTenantAppName)
        .add("consentUrl", consentUrl)
        .toString();
  }

  @Override
  public void validatePrefixForStorageType(String loc) {
    AzureLocation location = new AzureLocation(loc);
    Objects.requireNonNull(
        location); // do something with the variable so the JVM doesn't optimize out the check
  }
}
