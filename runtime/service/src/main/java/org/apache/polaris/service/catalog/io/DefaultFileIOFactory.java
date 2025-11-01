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
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.storage.StorageConfiguration;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}. For example, it evaluates storage actions and retrieves subscoped
 * credentials to initialize a {@link FileIO} instance with the most limited permissions necessary.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final AccessConfigProvider accessConfigProvider;
  private final StorageConfiguration storageConfiguration;
  private final Clock clock;

  @Inject
  public DefaultFileIOFactory(
      AccessConfigProvider accessConfigProvider,
      StorageConfiguration storageConfiguration,
      Clock clock) {
    this.accessConfigProvider = accessConfigProvider;
    this.storageConfiguration = storageConfiguration;
    this.clock = clock;
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {

    // TODO
    //    Boolean skipCredIndirection =
    //        realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);

    // Get subcoped creds
    properties = new HashMap<>(properties);
    AccessConfig accessConfig =
        accessConfigProvider.getAccessConfigForCredentialsVending(
            callContext,
            identifier,
            tableLocations,
            storageActions,
            Optional.empty(),
            resolvedEntityPath);

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    properties.putAll(accessConfig.credentials());
    properties.putAll(accessConfig.extraProperties());
    properties.putAll(accessConfig.internalProperties());

    // If no subscoped creds were produced, use system-wide AWS or GCP credentials if available.
    if (accessConfig.credentials().isEmpty()) {
      if (storageConfiguration.awsAccessKey().isPresent()
          && storageConfiguration.awsSecretKey().isPresent()) {
        // If no subscoped creds, use system-wide AWS credentials if available
        properties.put(
            StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
            storageConfiguration.awsAccessKey().get());
        properties.put(
            StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
            storageConfiguration.awsSecretKey().get());
      } else if (storageConfiguration.gcpAccessToken().isPresent()) {
        properties.put(
            StorageAccessProperty.GCS_ACCESS_TOKEN.getPropertyName(),
            storageConfiguration.gcpAccessToken().get());
        properties.put(
            StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT.getPropertyName(),
            String.valueOf(
                clock
                    .instant()
                    .plus(
                        storageConfiguration
                            .gcpAccessTokenLifespan()
                            .orElse(StorageConfiguration.DEFAULT_TOKEN_LIFESPAN))
                    .toEpochMilli()));
      }
    }

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    return new ExceptionMappingFileIO(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}
