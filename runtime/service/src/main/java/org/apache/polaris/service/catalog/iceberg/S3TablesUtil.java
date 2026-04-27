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
package org.apache.polaris.service.catalog.iceberg;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for S3 Tables credential vending in federated catalogs. Handles table ARN
 * construction, validation, and storage type detection for S3 Tables catalogs.
 */
public final class S3TablesUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3TablesUtil.class);

  /** ARN path segment separating the table bucket ARN from the tableId. */
  static final String TABLE_ARN_SEGMENT = "/table/";

  private S3TablesUtil() {}

  /** Checks whether the resolved catalog entity is configured with S3_TABLES storage type. */
  public static boolean isS3TablesCatalog(CatalogEntity catalogEntity) {
    PolarisStorageConfigurationInfo storageConfig = catalogEntity.getStorageConfigurationInfo();
    return storageConfig != null
        && storageConfig.getStorageType() == PolarisStorageConfigurationInfo.StorageType.S3_TABLES;
  }

  /**
   * Constructs an S3 Tables ARN from the catalog's default-base-location and a tableId. The
   * default-base-location for S3 Tables catalogs is the table bucket ARN (e.g.,
   * arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket). The resulting table ARN is
   * bucket-arn/table/tableId.
   */
  public static String constructTableArn(CatalogEntity catalogEntity, String tableId) {
    String baseLocation = catalogEntity.getBaseLocation();
    return baseLocation + TABLE_ARN_SEGMENT + tableId;
  }

  /**
   * Validates that a constructed S3 Tables ARN falls under one of the catalog's allowed locations.
   * This prevents a malicious remote catalog from returning a tableId that would construct an ARN
   * outside the catalog's authorized scope.
   */
  public static void validateTableArn(
      TableIdentifier tableIdentifier, String tableArn, CatalogEntity catalogEntity) {
    PolarisStorageConfigurationInfo storageConfig = catalogEntity.getStorageConfigurationInfo();
    if (storageConfig == null) {
      return;
    }
    List<String> allowedLocations = storageConfig.getAllowedLocations();
    boolean isAllowed =
        allowedLocations.stream().anyMatch(allowed -> tableArn.startsWith(allowed + "/"));
    if (!isAllowed) {
      throw new ForbiddenException(
          "Table '%s' has ARN '%s' which is outside the catalog's allowed locations: %s",
          tableIdentifier, tableArn, allowedLocations);
    }
  }

  /**
   * Resolves table locations for S3 Tables catalogs. If the catalog is S3_TABLES and a tableId was
   * captured from the remote loadTable response, replaces the s3:// table locations with the
   * constructed table ARN. If the catalog is S3_TABLES but no tableId is available, fails closed
   * with a BadRequestException.
   *
   * @param tableIdentifier the table being loaded
   * @param tableLocations the original s3:// table locations from metadata
   * @param catalogEntity the resolved catalog entity
   * @param capturedTableId the tableId captured from the remote catalog response
   * @return the resolved table locations (ARN for S3_TABLES, original locations otherwise)
   */
  public static Set<String> resolveTableLocations(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      CatalogEntity catalogEntity,
      Optional<String> capturedTableId) {

    if (!isS3TablesCatalog(catalogEntity)) {
      return tableLocations;
    }

    if (capturedTableId.isPresent()) {
      String tableArn = constructTableArn(catalogEntity, capturedTableId.get());
      validateTableArn(tableIdentifier, tableArn, catalogEntity);
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableArn", tableArn)
          .log("Replaced table locations with S3 Tables ARN for credential vending");
      return Set.of(tableArn);
    }

    // Fail closed: S3 Tables catalogs require a tableId to construct the table ARN
    // for scoped credential vending. Without it, we cannot generate a properly scoped
    // IAM session policy.
    throw new BadRequestException(
        "Cannot vend credentials for S3 Tables table '%s': "
            + "no tableId was captured from the remote catalog response. "
            + "Ensure the remote S3 Tables endpoint returns tableId in the loadTable config.",
        tableIdentifier);
  }
}
