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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.io.s3.ReflectionS3ClientInjector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFileIOFactory.class);

  private final StorageCredentialCache storageCredentialCache;
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public DefaultFileIOFactory(
      StorageCredentialCache storageCredentialCache,
      MetaStoreManagerFactory metaStoreManagerFactory) {
    this.storageCredentialCache = storageCredentialCache;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @Inject
  @Identifier("aws-sdk-http-client")
  SdkHttpClient sdkHttpClient;

  @Inject
  @Identifier("aws-sdk-http-client-insecure")
  SdkHttpClient insecureSdkHttpClient;

  @Override
  public FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
    RealmContext realmContext = callContext.getRealmContext();
    PolarisCredentialVendor credentialVendor =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    // Get subcoped creds
    properties = new HashMap<>(properties);
    Optional<PolarisEntity> storageInfoEntity =
        FileIOUtil.findStorageInfoFromHierarchy(resolvedEntityPath);
    Optional<AccessConfig> accessConfig =
        storageInfoEntity.map(
            storageInfo ->
                FileIOUtil.refreshAccessConfig(
                    callContext,
                    storageCredentialCache,
                    credentialVendor,
                    identifier,
                    tableLocations,
                    storageActions,
                    storageInfo,
                    Optional.empty()));

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    if (accessConfig.isPresent()) {
      properties.putAll(accessConfig.get().credentials());
      properties.putAll(accessConfig.get().extraProperties());
      properties.putAll(accessConfig.get().internalProperties());
    }

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    FileIO fileIO = CatalogUtil.loadFileIO(ioImplClassName, properties, null);

    // If this is Iceberg's S3FileIO and the storage config requested ignoring SSL verification,
    // try to construct and inject an S3 client that uses our insecure SDK HTTP client. This is
    // a best-effort reflective integration to make the 'ignoreSSLVerification' flag effective
    // for the S3 client used by Iceberg.
    try {
      boolean ignoreSsl = "true".equals(properties.get("polaris.ignore-ssl-verification"));
      if (ignoreSsl && "org.apache.iceberg.aws.s3.S3FileIO".equals(ioImplClassName)) {
        try {
          // Build prebuilt S3 clients using the insecure SDK HTTP client
          S3Client prebuilt =
              ReflectionS3ClientInjector.buildS3Client(insecureSdkHttpClient, properties);
          S3AsyncClient prebuiltAsync =
              ReflectionS3ClientInjector.buildS3AsyncClient(insecureSdkHttpClient, properties);
          boolean supplierInjected =
              ReflectionS3ClientInjector.injectSupplierIntoS3FileIO(
                  fileIO, prebuilt, prebuiltAsync);
          if (supplierInjected) {
            LOGGER.info(
                "Injected SerializableSupplier for insecure S3 client into Iceberg S3FileIO for ioImpl={}",
                ioImplClassName);
          } else {
            LOGGER.warn(
                "Requested ignore-ssl-verification but failed to inject insecure S3Client supplier into {}.\n"
                    + "Consider importing the storage certificate into Polaris JVM truststore or using HTTP endpoints for local testing.",
                ioImplClassName);
          }
        } catch (Throwable t) {
          LOGGER.warn(
              "Failed to build or inject prebuilt S3 client for {}: {}",
              ioImplClassName,
              t.getMessage());
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Exception while attempting to inject S3 client for {}: {}",
          ioImplClassName,
          e.toString());
    }

    return new ExceptionMappingFileIO(fileIO);
  }
}
