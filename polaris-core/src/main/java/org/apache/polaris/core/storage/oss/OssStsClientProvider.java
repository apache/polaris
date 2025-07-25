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

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for OSS STS clients with caching support using direct STS AssumeRole API
 */
public class OssStsClientProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(OssStsClientProvider.class);

  private final ConcurrentMap<StsDestination, IAcsClient> clientCache = new ConcurrentHashMap<>();
  private final int maxCacheSize;

  public OssStsClientProvider() {
    this(50); // Default cache size
  }

  public OssStsClientProvider(int maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
  }

  /**
   * Perform STS AssumeRole operation to get temporary credentials
   */
  public AssumeRoleResponse assumeRole(
      String roleArn, String roleSessionName, String externalId, String region,
      URI stsEndpoint, String policy, Long durationSeconds) throws ClientException {

    // Get environment variables for authentication
    String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

    if (accessKeyId == null || accessKeySecret == null) {
      throw new RuntimeException(
          "OSS credentials not available. Please set ALIBABA_CLOUD_ACCESS_KEY_ID " +
          "and ALIBABA_CLOUD_ACCESS_KEY_SECRET environment variables.");
    }

    LOGGER.debug("Using OSS credentials from environment variables");

    // Set default region if not provided
    String effectiveRegion = region != null ? region : "cn-hangzhou";

    // Set STS endpoint
    String endpoint = stsEndpoint != null ? stsEndpoint.getHost() : ("sts." + effectiveRegion + ".aliyuncs.com");

    try {
      // Add endpoint for STS service
      DefaultProfile.addEndpoint("", "Sts", endpoint);

      // Construct default profile
      IClientProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);

      // Construct client
      DefaultAcsClient client = new DefaultAcsClient(profile);

      // Create AssumeRole request
      final AssumeRoleRequest request = new AssumeRoleRequest();
      request.setSysMethod(MethodType.POST);
      request.setRoleArn(roleArn);
      request.setRoleSessionName(roleSessionName);

      if (policy != null) {
        request.setPolicy(policy);
      }

      if (durationSeconds != null) {
        request.setDurationSeconds(durationSeconds);
      }

      // Execute AssumeRole request
      final AssumeRoleResponse response = client.getAcsResponse(request);

      LOGGER.debug("Successfully assumed role: {}", roleArn);
      return response;

    } catch (ClientException e) {
      LOGGER.error("Failed to assume OSS role: roleArn={}, error={}", roleArn, e.getErrMsg());
      throw e;
    }
  }

  /**
   * Get or create STS client for the given destination with role configuration
   */
  public IAcsClient stsClient(@Nonnull StsDestination destination,
                              @Nonnull String roleArn,
                              @Nonnull String roleSessionName,
                              @Nullable String externalId) {
    StsClientKey key = new StsClientKey(destination, roleArn, roleSessionName, externalId);
    return clientCache.computeIfAbsent(destination, dest -> createClient(key));
  }

  private IAcsClient createClient(StsClientKey key) {
    // Evict entries if cache is full
    if (clientCache.size() >= maxCacheSize) {
      evictOldestEntry();
    }

    LOGGER.debug("Creating new OSS STS client for destination: {}, roleArn: {}",
        key.destination, key.roleArn);

    // Get environment variables for authentication
    String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

    if (accessKeyId == null || accessKeySecret == null) {
      throw new RuntimeException(
          "OSS credentials not available. Please set ALIBABA_CLOUD_ACCESS_KEY_ID " +
          "and ALIBABA_CLOUD_ACCESS_KEY_SECRET environment variables.");
    }

    String effectiveRegion = key.destination.region();

    // Set STS endpoint
    String endpoint = key.destination.endpoint() != null ?
        key.destination.endpoint().getHost() : ("sts." + effectiveRegion + ".aliyuncs.com");

    try {
      // Add endpoint for STS service
      DefaultProfile.addEndpoint("", "Sts", endpoint);

      // Construct default profile
      IClientProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);

      // Construct and return client
      return new DefaultAcsClient(profile);

    } catch (Exception e) {
      LOGGER.error("Failed to create STS client: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to create STS client", e);
    }
  }

  private void evictOldestEntry() {
    if (!clientCache.isEmpty()) {
      // Simple eviction strategy - remove first entry
      StsDestination firstKey = clientCache.keySet().iterator().next();
      clientCache.remove(firstKey);
      LOGGER.debug("Evicted STS client from cache for: {}", firstKey);
    }
  }

  /**
   * Clear all cached clients
   */
  public void clearCache() {
    clientCache.clear();
    LOGGER.debug("Cleared OSS STS client cache");
  }

  /**
   * Get current cache size
   */
  public int getCacheSize() {
    return clientCache.size();
  }

  /**
   * Represents a destination for STS client creation
   */
  public static class StsDestination {
    private final String region;
    private final URI endpoint;

    private StsDestination(String region, URI endpoint) {
      this.region = region;
      this.endpoint = endpoint;
    }

    public static StsDestination of(@Nullable URI endpoint, @Nullable String region) {
      String effectiveRegion = region != null ? region : "cn-hangzhou";
      return new StsDestination(effectiveRegion, endpoint);
    }

    public String region() {
      return region;
    }

    public URI endpoint() {
      return endpoint;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StsDestination that = (StsDestination) o;
      return Objects.equals(region, that.region) && Objects.equals(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
      return Objects.hash(region, endpoint);
    }

    @Override
    public String toString() {
      return "StsDestination{region='" + region + "', endpoint=" + endpoint + '}';
    }
  }

  /**
   * Key for caching STS clients with role information
   */
  private static class StsClientKey {
    private final StsDestination destination;
    private final String roleArn;
    private final String roleSessionName;
    private final String externalId;

    StsClientKey(StsDestination destination, String roleArn, String roleSessionName, String externalId) {
      this.destination = destination;
      this.roleArn = roleArn;
      this.roleSessionName = roleSessionName;
      this.externalId = externalId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StsClientKey that = (StsClientKey) o;
      return Objects.equals(destination, that.destination) &&
             Objects.equals(roleArn, that.roleArn) &&
             Objects.equals(roleSessionName, that.roleSessionName) &&
             Objects.equals(externalId, that.externalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(destination, roleArn, roleSessionName, externalId);
    }
  }
}