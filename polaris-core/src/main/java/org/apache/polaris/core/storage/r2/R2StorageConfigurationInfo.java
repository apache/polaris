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
package org.apache.polaris.core.storage.r2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

/**
 * Cloudflare R2 storage configuration. R2 speaks the S3 API, so locations use {@code s3://} and the
 * FileIO is {@code S3FileIO}; the endpoint host and the audience of vended credentials are derived
 * from {@link #getAccountId()} and the optional {@link #getJurisdiction()}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableR2StorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableR2StorageConfigurationInfo.class)
@tools.jackson.databind.annotation.JsonSerialize(as = ImmutableR2StorageConfigurationInfo.class)
@tools.jackson.databind.annotation.JsonDeserialize(as = ImmutableR2StorageConfigurationInfo.class)
@JsonTypeName("R2StorageConfigurationInfo")
public abstract class R2StorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static final String R2_HOST_SUFFIX = "r2.cloudflarestorage.com";

  /** Jurisdictions documented at developers.cloudflare.com/r2/reference/data-location/. */
  public static final List<String> KNOWN_JURISDICTIONS = List.of("eu", "fedramp", "us");

  private static final Pattern ACCOUNT_ID_PATTERN = Pattern.compile("^[0-9a-f]{32}$");

  public static ImmutableR2StorageConfigurationInfo.Builder builder() {
    return ImmutableR2StorageConfigurationInfo.builder();
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.R2;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  /** Cloudflare account id (32 lowercase hex characters). Required. */
  @Nullable
  public abstract String getAccountId();

  /** Optional jurisdiction token, one of {@link #KNOWN_JURISDICTIONS}. */
  @Nullable
  public abstract String getJurisdiction();

  /** Host of the S3 endpoint; also the {@code aud} claim of vended credentials. */
  @JsonIgnore
  public String getEndpointHost() {
    String jurisdiction = getJurisdiction();
    return jurisdiction == null
        ? getAccountId() + "." + R2_HOST_SUFFIX
        : getAccountId() + "." + jurisdiction + "." + R2_HOST_SUFFIX;
  }

  @JsonIgnore
  public URI getEndpointUri() {
    return URI.create("https://" + getEndpointHost());
  }

  @Value.Check
  @Override
  protected void check() {
    super.check();
    String accountId = getAccountId();
    if (accountId == null || !ACCOUNT_ID_PATTERN.matcher(accountId).matches()) {
      throw new IllegalArgumentException(
          "R2 storage config requires accountId matching ^[0-9a-f]{32}$");
    }
    String jurisdiction = getJurisdiction();
    if (jurisdiction != null && !KNOWN_JURISDICTIONS.contains(jurisdiction)) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown R2 jurisdiction '%s'; accepted values: %s",
              jurisdiction, String.join(", ", KNOWN_JURISDICTIONS)));
    }
  }
}
