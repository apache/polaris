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
package org.apache.polaris.service.storage.gcs;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.control.v2.BucketName;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.StorageType;
import org.apache.polaris.service.storage.AbstractStorageLocationPreparer;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS implementation of {@link AbstractStorageLocationPreparer}. Detects whether the target bucket
 * uses Hierarchical Namespace (HNS) and, if so, creates the full folder hierarchy required before
 * Iceberg metadata and data files can be written. For flat-namespace buckets, this is a no-op.
 */
@ApplicationScoped
@Identifier("GCS")
public class GcsStorageLocationPreparer extends AbstractStorageLocationPreparer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorageLocationPreparer.class);
  private static final String GCS_STORAGE_LAYOUT = "_";

  private final Supplier<GoogleCredentials> credentialsSupplier;

  @Inject
  public GcsStorageLocationPreparer(StorageConfiguration storageConfiguration, Clock clock) {
    this.credentialsSupplier = storageConfiguration.gcpCredentialsSupplier(clock);
  }

  /** Package-private constructor for testing. */
  GcsStorageLocationPreparer(Supplier<GoogleCredentials> credentialsSupplier) {
    this.credentialsSupplier = credentialsSupplier;
  }

  @Override
  protected String schemePrefix() {
    return StorageType.GCS.getPrefixes().getFirst();
  }

  // ── HNS Detection & Folder Creation ────────────────────────────────────────

  @Override
  protected void createFoldersForBucket(String bucketName, List<FolderToCreate> folders) {
    boolean hnsEnabled = resolveHnsStatus(bucketName);
    LOGGER
        .atInfo()
        .addKeyValue("bucket", bucketName)
        .addKeyValue("hnsEnabled", hnsEnabled)
        .addKeyValue("folderCount", folders.size())
        .log("GCS bucket HNS status for table creation");

    if (hnsEnabled) {
      createHnsFoldersInBucket(bucketName, folders);
    }
  }

  /** Creates all folders in a bucket using a single StorageControlClient instance. */
  private void createHnsFoldersInBucket(String bucketName, List<FolderToCreate> folders) {
    try (StorageControlClient controlClient = createStorageControlClient()) {
      String parent = BucketName.format(GCS_STORAGE_LAYOUT, bucketName);
      folders.forEach(
          folder -> createFolderIfNotExists(controlClient, parent, folder.folderPath()));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to initialize StorageControlClient for HNS folder creation in bucket '%s'",
              bucketName),
          e);
    } catch (RuntimeException e) {
      if (isGrpcConfigurationError(e)) {
        LOGGER
            .atWarn()
            .addKeyValue("bucket", bucketName)
            .addKeyValue("folderCount", folders.size())
            .addKeyValue("error", e.getMessage())
            .log(
                "Failed to create HNS folders due to gRPC/networking configuration issue; "
                    + "table creation may fail if folders don't exist",
                e);
      } else {
        throw e;
      }
    }
  }

  /**
   * Checks if the exception is related to gRPC configuration issues that should be handled
   * gracefully.
   */
  private boolean isGrpcConfigurationError(RuntimeException e) {
    String message = e.getMessage();
    Throwable cause = e.getCause();

    return (message != null
            && (message.contains("census")
                || message.contains("JNDI")
                || message.contains("grpc")
                || message.contains("Unable to apply census stats")))
        || (cause != null
            && (cause instanceof ClassNotFoundException
                || cause instanceof javax.naming.NamingException));
  }

  private boolean resolveHnsStatus(String bucketName) {
    try {
      Bucket bucket = fetchBucketMetadata(bucketName);
      return Optional.ofNullable(bucket.getHierarchicalNamespace())
          .map(BucketInfo.HierarchicalNamespace::getEnabled)
          .orElse(false);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Failed to check HNS status for GCS bucket '%s'. "
                  + "Table creation is blocked until the bucket status can be verified.",
              bucketName),
          e);
    }
  }

  Bucket fetchBucketMetadata(String bucketName) {
    Storage storage =
        StorageOptions.newBuilder().setCredentials(credentialsSupplier.get()).build().getService();

    return Optional.ofNullable(
            storage.get(
                bucketName,
                Storage.BucketGetOption.fields(Storage.BucketField.HIERARCHICAL_NAMESPACE)))
        .orElseThrow(
            () ->
                new RuntimeException(
                    String.format(
                        "GCS bucket '%s' not found. Cannot proceed with table creation.",
                        bucketName)));
  }

  private void createFolderIfNotExists(
      StorageControlClient controlClient, String parent, String folderPath) {
    try {
      CreateFolderRequest request =
          CreateFolderRequest.newBuilder().setParent(parent).setFolderId(folderPath).build();

      controlClient.createFolder(request);
      LOGGER.atDebug().addKeyValue("folder", folderPath).log("Created HNS folder");
    } catch (AlreadyExistsException e) {
      LOGGER.atDebug().addKeyValue("folder", folderPath).log("HNS folder already exists, skipping");
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create HNS folder '%s': %s", folderPath, e.getMessage()), e);
    }
  }

  StorageControlClient createStorageControlClient() throws IOException {
    GoogleCredentials credentials = credentialsSupplier.get();
    StorageControlSettings settings =
        StorageControlSettings.newBuilder().setCredentialsProvider(() -> credentials).build();
    return StorageControlClient.create(settings);
  }
}
