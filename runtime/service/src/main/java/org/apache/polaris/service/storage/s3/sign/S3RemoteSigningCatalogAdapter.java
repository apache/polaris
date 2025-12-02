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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.function.Function;
import org.apache.iceberg.aws.s3.signer.S3SignResponse;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.s3.sign.api.IcebergRestS3SignerApiService;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A bridge between {@link IcebergRestS3SignerApiService} and {@link CatalogAdapter}. */
@RequestScoped
public class S3RemoteSigningCatalogAdapter
    implements IcebergRestS3SignerApiService, CatalogAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3RemoteSigningCatalogAdapter.class);

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisAuthorizer polarisAuthorizer;
  @Inject CallContextCatalogFactory catalogFactory;
  @Inject CatalogPrefixParser prefixParser;
  @Inject S3RequestSigner s3RequestSigner;
  @Inject PolarisPrincipal polarisPrincipal;

  /**
   * Execute operations on a catalog wrapper and ensure we close the BaseCatalog afterward. This
   * will typically ensure the underlying FileIO is closed.
   */
  private Response withCatalog(
      SecurityContext securityContext,
      String prefix,
      Function<S3RemoteSigningCatalogHandler, Response> action) {
    validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    try (S3RemoteSigningCatalogHandler handler =
        new S3RemoteSigningCatalogHandler(
            diagnostics,
            callContext,
            resolutionManifestFactory,
            polarisPrincipal,
            catalogFactory,
            catalogName,
            polarisAuthorizer,
            s3RequestSigner)) {
      return action.apply(handler);
    } catch (RuntimeException e) {
      LOGGER.debug("RuntimeException while operating on catalog. Propagating to caller.", e);
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error while operating on catalog", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Response signS3Request(
      String prefix,
      String namespace,
      String table,
      PolarisS3SignRequest s3SignRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          S3SignResponse response = catalog.signS3Request(s3SignRequest, tableIdentifier);
          return Response.status(Response.Status.OK).entity(response).build();
        });
  }
}
