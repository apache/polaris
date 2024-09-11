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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value.Check;
import org.jetbrains.annotations.Nullable;

/** Gcp storage storage configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableGcpStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableGcpStorageConfigurationInfo.class)
public abstract class GcpStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 8 is an experimental result from generating GCP accessBoundaryRules when subscoping creds,
  // when the rule is too large, GCS only returns error: 400 bad request "Invalid arguments
  // provided in the request"
  private static final int MAX_ALLOWED_LOCATIONS = 8;

  public static GcpStorageConfigurationInfo of(Iterable<String> allowedLocations) {
    return ImmutableGcpStorageConfigurationInfo.builder()
        .allowedLocations(allowedLocations)
        .build();
  }

  @Override
  public abstract List<String> getAllowedLocations();

  @Override
  public StorageType getStorageType() {
    return StorageType.GCS;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.gcp.gcs.GCSFileIO";
  }

  /** The gcp service account. */
  @Nullable
  public abstract String getGcpServiceAccount();

  @Check
  @Override
  protected void validate() {
    super.validate();
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }
}
