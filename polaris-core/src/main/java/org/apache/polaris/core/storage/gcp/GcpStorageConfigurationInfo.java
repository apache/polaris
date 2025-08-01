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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;

/** Gcp storage storage configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableGcpStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableGcpStorageConfigurationInfo.class)
@JsonTypeName("GcpStorageConfigurationInfo")
public abstract class GcpStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableGcpStorageConfigurationInfo.Builder builder() {
    return ImmutableGcpStorageConfigurationInfo.builder();
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.gcp.gcs.GCSFileIO";
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.GCS;
  }

  /** The gcp service account */
  @Nullable
  public abstract String getGcpServiceAccount();
}
