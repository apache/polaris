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
package org.apache.polaris.service.storage.aws.signer;

import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.service.aws.sign.model.PolarisS3SignRequest;
import org.apache.polaris.service.aws.sign.model.PolarisS3SignResponse;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RemoteSigningCatalogHandler extends CatalogHandler implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3RemoteSigningCatalogHandler.class);

  private final S3RequestSigner s3RequestSigner;

  private CatalogEntity catalogEntity;

  public S3RemoteSigningCatalogHandler(
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer,
      S3RequestSigner s3RequestSigner) {
    super(callContext, resolutionManifestFactory, securityContext, catalogName, authorizer);
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

    LOGGER.debug("Requesting s3 signing for {}: {}", tableIdentifier, s3SignRequest);

    throwIfRemoteSigningNotEnabled(callContext.getRealmConfig(), catalogEntity);

    // TODO authorize based on the request's method?

    try {
      authorizeBasicTableLikeOperationOrThrow(
          PolarisAuthorizableOperation.SIGN_S3_REQUEST,
          PolarisEntitySubType.ICEBERG_TABLE,
          tableIdentifier);
    } catch (NoSuchTableException e) {
      // FIXME can we do better here?
      authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.SIGN_S3_REQUEST, tableIdentifier);
    }

    PolarisS3SignResponse s3SignResponse = s3RequestSigner.signRequest(s3SignRequest);
    LOGGER.debug("S3 signing response: {}", s3SignResponse);

    return s3SignResponse;
  }

  public static void throwIfRemoteSigningNotEnabled(
      RealmConfig realmConfig, CatalogEntity catalogEntity) {
    boolean remoteSigningEnabled =
        realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_ENABLED, catalogEntity);
    if (!remoteSigningEnabled) {
      throw new ForbiddenException(
          "Remote signing is not enabled for this catalog. To enable this feature, set the Polaris configuration %s "
              + "or the catalog configuration %s.",
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.key(),
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.catalogConfig());
    }
    if (catalogEntity.isExternal()) {
      remoteSigningEnabled =
          realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_EXTERNAL_CATALOGS_ENABLED);
      if (!remoteSigningEnabled) {
        throw new ForbiddenException(
            "Remote signing is not enabled for external catalogs. To enable this feature, set the Polaris configuration %s.",
            FeatureConfiguration.REMOTE_SIGNING_EXTERNAL_CATALOGS_ENABLED.key());
      }
    }
  }

  @Override
  public void close() {}
}
