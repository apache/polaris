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
package org.apache.polaris.service.storage.s3.sign;

import java.util.EnumSet;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.storage.LocationRestrictions;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignRequest;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RemoteSigningCatalogHandler extends CatalogHandler implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3RemoteSigningCatalogHandler.class);

  private final S3RequestSigner s3RequestSigner;

  private CatalogEntity catalogEntity;

  public S3RemoteSigningCatalogHandler(
      PolarisDiagnostics diagnostics,
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal polarisPrincipal,
      String catalogName,
      PolarisAuthorizer authorizer,
      S3RequestSigner s3RequestSigner) {
    super(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        polarisPrincipal,
        catalogName,
        authorizer,
        // external catalogs are not supported for S3 remote signing
        null,
        null);
    this.s3RequestSigner = s3RequestSigner;
  }

  @Override
  protected void initializeCatalog() {
    catalogEntity =
        CatalogEntity.of(resolutionManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    if (catalogEntity.isExternal()) {
      throw new ForbiddenException("Cannot use S3 remote signing with federated catalogs.");
    }
    // no need to materialize the catalog here, as we only need the catalog entity
  }

  public PolarisS3SignResponse signS3Request(
      PolarisS3SignRequest s3SignRequest, TableIdentifier tableIdentifier) {

    PolarisAuthorizableOperation authzOp =
        s3SignRequest.write()
            ? PolarisAuthorizableOperation.SIGN_S3_WRITE_REQUEST
            : PolarisAuthorizableOperation.SIGN_S3_READ_REQUEST;

    authorizeRemoteSigningOrThrow(EnumSet.of(authzOp), tableIdentifier);

    // Must be done after the authorization check, as the auth check creates the catalog entity;
    // also, materializing the catalog here could hurt performance.
    throwIfRemoteSigningNotEnabled(callContext.getRealmConfig(), catalogEntity);

    validateLocations(s3SignRequest, tableIdentifier);

    return s3RequestSigner.signRequest(s3SignRequest);
  }

  private void validateLocations(
      PolarisS3SignRequest s3SignRequest, TableIdentifier tableIdentifier) {

    // Will point to the table entity if it exists, otherwise the namespace entity.
    PolarisResolvedPathWrapper tableOrNamespace =
        CatalogUtils.findResolvedStorageEntity(resolutionManifest, tableIdentifier);

    Set<String> targetLocations = getTargetLocations(s3SignRequest);

    // If the table exists already, validate the target locations against the table's locations;
    // otherwise, validate against the namespace's locations using the entity path hierarchy.
    if (tableOrNamespace.getRawLeafEntity().getType() == PolarisEntityType.TABLE_LIKE) {
      TableLikeEntity tableEntity = new IcebergTableLikeEntity(tableOrNamespace.getRawLeafEntity());
      Set<String> allowedLocations =
          StorageUtil.getLocationsUsedByTable(
              tableEntity.getBaseLocation(), tableEntity.getPropertiesAsMap());
      new LocationRestrictions(allowedLocations)
          .validate(callContext.getRealmConfig(), tableIdentifier, targetLocations);
    } else {
      CatalogUtils.validateLocationsForTableLike(
          callContext.getRealmConfig(), tableIdentifier, targetLocations, tableOrNamespace);
    }
  }

  private Set<String> getTargetLocations(PolarisS3SignRequest s3SignRequest) {
    // TODO M2: map http URI to s3 URI
    return Set.of();
  }

  public static void throwIfRemoteSigningNotEnabled(
      RealmConfig realmConfig, CatalogEntity catalogEntity) {
    if (catalogEntity.isExternal()) {
      throw new ForbiddenException("Remote signing is not enabled for external catalogs.");
    }
    boolean remoteSigningEnabled =
        realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_ENABLED, catalogEntity);
    if (!remoteSigningEnabled) {
      throw new ForbiddenException(
          "Remote signing is not enabled for this catalog. To enable this feature, set the Polaris configuration %s "
              + "or the catalog configuration %s.",
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.key(),
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.catalogConfig());
    }
  }

  @Override
  public void close() {}
}
