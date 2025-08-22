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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.ApiBusinessLogic;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.CreateGenericTableRequest;

@RequestScoped
@Alternative
@Priority(1000) // Will allow vendor-specific delegators to be added and used
public class GenericTableCatalogAdapterDefaultDelegator implements PolarisCatalogGenericTableApiService, CatalogAdapter {
    private final GenericTableCatalogAdapter delegate;

    @Inject
    public GenericTableCatalogAdapterDefaultDelegator(@ApiBusinessLogic GenericTableCatalogAdapter delegate) {
        this.delegate = delegate;
    }

    @Override
    public Response createGenericTable(
            String prefix,
            String namespace,
            CreateGenericTableRequest createGenericTableRequest,
            RealmContext realmContext,
            SecurityContext securityContext) {
        return delegate.createGenericTable(prefix, namespace, createGenericTableRequest, realmContext, securityContext);
    }

    @Override
    public Response dropGenericTable(
            String prefix,
            String namespace,
            String genericTable,
            RealmContext realmContext,
            SecurityContext securityContext) {
        return delegate.dropGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    }

    @Override
    public Response listGenericTables(
            String prefix,
            String namespace,
            String pageToken,
            Integer pageSize,
            RealmContext realmContext,
            SecurityContext securityContext) {
        return delegate.listGenericTables(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    }

    @Override
    public Response loadGenericTable(
            String prefix,
            String namespace,
            String genericTable,
            RealmContext realmContext,
            SecurityContext securityContext) {
        return delegate.loadGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    }
}
