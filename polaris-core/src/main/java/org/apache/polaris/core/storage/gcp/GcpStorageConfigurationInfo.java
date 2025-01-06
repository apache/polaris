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
package org.apache.polaris.core.storage.gcp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/** Gcp storage storage configuration information. */
public class GcpStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 8 is an experimental result from generating GCP accessBoundaryRules when subscoping creds,
  // when the rule is too large, GCS only returns error: 400 bad request "Invalid arguments
  // provided in the request"
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 8;

  /** The gcp service account */
  @JsonProperty(value = "gcpServiceAccount", required = false)
  private @Nullable String gcpServiceAccount = null;

  @JsonCreator
  public GcpStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @Nonnull
          List<String> allowedLocations) {
    super(StorageType.GCS, allowedLocations);
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.gcp.gcs.GCSFileIO";
  }

  public void setGcpServiceAccount(@Nullable String gcpServiceAccount) {
    this.gcpServiceAccount = gcpServiceAccount;
  }

  public @Nullable String getGcpServiceAccount() {
    return gcpServiceAccount;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", getStorageType())
        .add("allowedLocation", getAllowedLocations())
        .add("gcpServiceAccount", gcpServiceAccount)
        .toString();
  }
}
