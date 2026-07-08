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
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.jspecify.annotations.NonNull;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}.
 *
 * <p>When constructed with {@link S3AccessConfig} (CDI {@code @Identifier("default")} and other
 * production factories such as {@code wasb}), {@code polaris.storage.*} HTTP client settings are
 * applied to Iceberg AWS FileIOs. The no-arg constructor is for tests and fixtures that do not need
 * those settings.
 */
@RequestScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final S3AccessConfig s3AccessConfig;

  /** For tests and fixtures that do not need {@code polaris.storage.*} HTTP client settings. */
  public DefaultFileIOFactory() {
    this(null);
  }

  @Inject
  public DefaultFileIOFactory(S3AccessConfig s3AccessConfig) {
    this.s3AccessConfig = s3AccessConfig;
  }

  @Override
  public FileIO loadFileIO(
      @NonNull StorageAccessConfig storageAccessConfig,
      @NonNull String ioImplClassName,
      @NonNull Map<String, String> properties) {

    // Get subcoped creds
    Map<String, String> props = new HashMap<>(properties);

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    props.putAll(storageAccessConfig.credentials());
    props.putAll(storageAccessConfig.extraProperties());
    props.putAll(storageAccessConfig.internalProperties());

    // Apply polaris.storage.* HTTP client settings to Iceberg S3FileIO (and other AWS FileIOs).
    // Previously only applied to the STS client pool. Skipped when constructed without config
    // (tests / fixtures); production CDI paths always inject S3AccessConfig.
    if (s3AccessConfig != null) {
      s3AccessConfig
          .maxHttpConnections()
          .ifPresent(v -> props.put("http-client.apache.max-connections", String.valueOf(v)));
      s3AccessConfig
          .readTimeout()
          .ifPresent(
              d -> props.put("http-client.apache.socket-timeout-ms", String.valueOf(d.toMillis())));
      s3AccessConfig
          .connectTimeout()
          .ifPresent(
              d ->
                  props.put(
                      "http-client.apache.connection-timeout-ms", String.valueOf(d.toMillis())));
      s3AccessConfig
          .connectionAcquisitionTimeout()
          .ifPresent(
              d ->
                  props.put(
                      "http-client.apache.connection-acquisition-timeout-ms",
                      String.valueOf(d.toMillis())));
      s3AccessConfig
          .connectionMaxIdleTime()
          .ifPresent(
              d ->
                  props.put(
                      "http-client.apache.connection-max-idle-time-ms",
                      String.valueOf(d.toMillis())));
      s3AccessConfig
          .connectionTimeToLive()
          .ifPresent(
              d ->
                  props.put(
                      "http-client.apache.connection-time-to-live-ms",
                      String.valueOf(d.toMillis())));
      s3AccessConfig
          .expectContinueEnabled()
          .ifPresent(
              v -> props.put("http-client.apache.expect-continue-enabled", String.valueOf(v)));
    }

    return loadFileIOInternal(ioImplClassName, props);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @NonNull String ioImplClassName, @NonNull Map<String, String> properties) {
    return new ExceptionMappingFileIO(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}
