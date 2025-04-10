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
package org.apache.polaris.service.catalog.generic;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.types.CreateGenericTableRequest;

@RequestScoped
public class GenericTableCatalogAdapter implements PolarisCatalogGenericTableApiService {
  private final RealmContext realmContext;
  private final CallContext callContext;
  private final CallContextCatalogFactory catalogFactory;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisAuthorizer polarisAuthorizer;
  private final CatalogPrefixParser prefixParser;

  @Inject
  public GenericTableCatalogAdapter(
      RealmContext realmContext,
      CallContext callContext,
      CallContextCatalogFactory catalogFactory,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      PolarisAuthorizer polarisAuthorizer,
      CatalogPrefixParser prefixParser) {
    this.realmContext = realmContext;
    this.callContext = callContext;
    this.catalogFactory = catalogFactory;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.polarisAuthorizer = polarisAuthorizer;
    this.prefixParser = prefixParser;

    // FIXME: This is a hack to set the current context for downstream calls.
    CallContext.setCurrentContext(callContext);
  }

  private GenericTableCatalogHandler newHandlerWrapper(
      SecurityContext securityContext, String catalogName) {
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    return new GenericTableCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext,
        catalogName,
        polarisAuthorizer);
  }

  private static Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);

    return Response.ok(
            newHandlerWrapper(securityContext, prefix)
                .createGenericTable(
                    TableIdentifier.of(ns, createGenericTableRequest.getName()),
                    createGenericTableRequest.getFormat(),
                    createGenericTableRequest.getDoc(),
                    createGenericTableRequest.getProperties()))
        .build();
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }

  @Override
  public Response listGenericTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(genericTable));
    return Response.ok(newHandlerWrapper(securityContext, prefix).loadGenericTable(tableIdentifier))
        .build();
  }
}
