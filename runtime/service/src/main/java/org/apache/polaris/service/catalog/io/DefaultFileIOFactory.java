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
package org.apache.polaris.service.catalog.io;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.jspecify.annotations.NonNull;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances used by the
 * Polaris server.
 *
 * <p>Merge order: contextual properties, then {@link StorageAccessConfig} credentials /
 * extraProperties / internalProperties (AccessConfig wins), then {@code polaris.storage.*} HTTP
 * client settings from {@link S3AccessConfig}. Call sites must pass catalog-trusted context (for
 * example {@code table-default.*}), not table {@code metadata.properties()}.
 *
 * <p>Production CDI paths inject the live {@link S3AccessConfig}; tests/fixtures can pass {@link
 * S3AccessConfig#empty()}.
 */
@RequestScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final S3AccessConfig s3AccessConfig;

  @Inject
  public DefaultFileIOFactory(S3AccessConfig s3AccessConfig) {
    this.s3AccessConfig = s3AccessConfig;
  }

  @Override
  public FileIO loadFileIO(
      @NonNull StorageAccessConfig storageAccessConfig,
      @NonNull String ioImplClassName,
      @NonNull Map<String, String> tableProperties) {

    // Contextual properties first (e.g. catalog table-default.*). AccessConfig always overlays so
    // storage-config credentials/endpoint win. Call sites must not pass table metadata.properties()
    // (caller-controlled FileIO client keys such as s3.endpoint).
    Map<String, String> properties = new HashMap<>(tableProperties);
    properties.putAll(storageAccessConfig.credentials());
    properties.putAll(storageAccessConfig.extraProperties());
    properties.putAll(storageAccessConfig.internalProperties());

    // Apply polaris.storage.* HTTP client settings to Iceberg S3FileIO (and other AWS FileIOs).
    // Previously only applied to the STS client pool. Empty config is a no-op (tests/fixtures).
    s3AccessConfig
        .maxHttpConnections()
        .ifPresent(
            v -> properties.put(HttpClientProperties.APACHE_MAX_CONNECTIONS, String.valueOf(v)));
    s3AccessConfig
        .readTimeout()
        .ifPresent(
            d ->
                properties.put(
                    HttpClientProperties.APACHE_SOCKET_TIMEOUT_MS, String.valueOf(d.toMillis())));
    s3AccessConfig
        .connectTimeout()
        .ifPresent(
            d ->
                properties.put(
                    HttpClientProperties.APACHE_CONNECTION_TIMEOUT_MS,
                    String.valueOf(d.toMillis())));
    s3AccessConfig
        .connectionAcquisitionTimeout()
        .ifPresent(
            d ->
                properties.put(
                    HttpClientProperties.APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS,
                    String.valueOf(d.toMillis())));
    s3AccessConfig
        .connectionMaxIdleTime()
        .ifPresent(
            d ->
                properties.put(
                    HttpClientProperties.APACHE_CONNECTION_MAX_IDLE_TIME_MS,
                    String.valueOf(d.toMillis())));
    s3AccessConfig
        .connectionTimeToLive()
        .ifPresent(
            d ->
                properties.put(
                    HttpClientProperties.APACHE_CONNECTION_TIME_TO_LIVE_MS,
                    String.valueOf(d.toMillis())));
    s3AccessConfig
        .expectContinueEnabled()
        .ifPresent(
            v ->
                properties.put(
                    HttpClientProperties.APACHE_EXPECT_CONTINUE_ENABLED, String.valueOf(v)));

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @NonNull String ioImplClassName, @NonNull Map<String, String> properties) {
    return ExceptionMappingFileIO.wrap(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}
