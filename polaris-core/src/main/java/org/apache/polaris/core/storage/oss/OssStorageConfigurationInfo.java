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
package org.apache.polaris.core.storage.oss;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/** OSS Polaris Storage Configuration information */
public class OssStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

    @JsonIgnore
    public static final String ROLE_ARN_PATTERN = "^acs:ram::[0-9]+:role/.+$";
    private static final Pattern ROLE_ARN_PATTERN_COMPILED = Pattern.compile(ROLE_ARN_PATTERN);

    // OSS RAM role to be assumed
    private final @Nonnull String roleArn;

    // OSS external ID, optional
    @JsonProperty(value = "externalId")
    private @Nullable String externalId = null;

    // OSS region
    @JsonProperty(value = "region")
    private @Nullable String region = null;

    // OSS endpoint
    @JsonProperty(value = "endpoint")
    private @Nullable String endpoint = null;

    // STS endpoint
    @JsonProperty(value = "stsEndpoint")
    private @Nullable String stsEndpoint = null;

    @JsonCreator
    public OssStorageConfigurationInfo(
            @JsonProperty(value = "storageType", required = true) @Nonnull StorageType storageType,
            @JsonProperty(value = "allowedLocations", required = true) @Nonnull List<String> allowedLocations,
            @JsonProperty(value = "roleArn", required = true) @Nonnull String roleArn,
            @JsonProperty(value = "externalId") @Nullable String externalId,
            @JsonProperty(value = "region") @Nullable String region,
            @JsonProperty(value = "endpoint") @Nullable String endpoint,
            @JsonProperty(value = "stsEndpoint") @Nullable String stsEndpoint) {
        super(storageType, allowedLocations);
        validateArn(roleArn);
        this.roleArn = roleArn;
        this.externalId = externalId;
        this.region = region;
        this.endpoint = endpoint;
        this.stsEndpoint = stsEndpoint;
    }

    public OssStorageConfigurationInfo(
            @Nonnull StorageType storageType,
            @Nonnull List<String> allowedLocations,
            @Nonnull String roleArn,
            @Nullable String region) {
        this(storageType, allowedLocations, roleArn, null, region, null, null);
    }

    public OssStorageConfigurationInfo(
            @Nonnull StorageType storageType,
            @Nonnull List<String> allowedLocations,
            @Nonnull String roleArn,
            @Nullable String externalId,
            @Nullable String region) {
        this(storageType, allowedLocations, roleArn, externalId, region, null, null);
    }

    @Override
    public String getFileIoImplClassName() {
        return "org.apache.iceberg.aliyun.oss.OSSFileIO";
    }

    public static void validateArn(String arn) {
        if (arn == null || arn.isEmpty()) {
            throw new IllegalArgumentException("ARN cannot be null or empty");
        }
        if (!ROLE_ARN_PATTERN_COMPILED.matcher(arn).matches()) {
            throw new IllegalArgumentException("Invalid OSS role ARN format");
        }
    }

    public @Nonnull String getRoleArn() {
        return roleArn;
    }

    public @Nullable String getExternalId() {
        return externalId;
    }

    public void setExternalId(@Nullable String externalId) {
        this.externalId = externalId;
    }

    public @Nullable String getRegion() {
        return region;
    }

    public @Nullable URI getEndpointUri() {
        return endpoint == null ? null : URI.create(endpoint);
    }

    public @Nullable URI getStsEndpointUri() {
        if (stsEndpoint != null) {
            return URI.create(stsEndpoint);
        }
        return endpoint == null ? null : URI.create(endpoint);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("storageType", getStorageType())
                .add("allowedLocations", getAllowedLocations())
                .add("roleArn", roleArn)
                .add("externalId", externalId)
                .add("region", region)
                .add("endpoint", endpoint)
                .add("stsEndpoint", stsEndpoint)
                .toString();
    }
}