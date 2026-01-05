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
package org.apache.polaris.service.events;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.UpdatePolicyRequest;

/** Whitelist of types allowed for event attributes. */
final class AllowedAttributeTypes {
  private AllowedAttributeTypes() {}

  static final Set<Class<?>> ALLOWED_TYPES =
      Set.of(
          // Primitives
          String.class,
          Boolean.class,
          Integer.class,
          Long.class,
          Double.class,
          // Collections
          List.class,
          Map.class,
          Set.class,
          // Iceberg catalog types
          Namespace.class,
          TableIdentifier.class,
          // Iceberg metadata types
          TableMetadata.class,
          ViewMetadata.class,
          // Iceberg REST request types
          CreateNamespaceRequest.class,
          UpdateNamespacePropertiesRequest.class,
          CreateTableRequest.class,
          UpdateTableRequest.class,
          RegisterTableRequest.class,
          RenameTableRequest.class,
          CreateViewRequest.class,
          CommitTransactionRequest.class,
          // Iceberg REST response types
          UpdateNamespacePropertiesResponse.class,
          LoadTableResponse.class,
          LoadViewResponse.class,
          ConfigResponse.class,
          // Polaris admin model types
          Catalog.class,
          Principal.class,
          PrincipalRole.class,
          PrincipalWithCredentials.class,
          CatalogRole.class,
          GrantResource.class,
          CreatePrincipalRequest.class,
          UpdatePrincipalRequest.class,
          CreatePrincipalRoleRequest.class,
          UpdatePrincipalRoleRequest.class,
          UpdateCatalogRequest.class,
          UpdateCatalogRoleRequest.class,
          AddGrantRequest.class,
          RevokeGrantRequest.class,
          // Polaris core types
          PolarisPrivilege.class,
          // Polaris service types
          CommitViewRequest.class,
          GenericTable.class,
          CreateGenericTableRequest.class,
          CreatePolicyRequest.class,
          UpdatePolicyRequest.class,
          LoadPolicyResponse.class,
          AttachPolicyRequest.class,
          DetachPolicyRequest.class,
          GetApplicablePoliciesResponse.class,
          NotificationRequest.class);

  static boolean isAllowed(Class<?> type) {
    return ALLOWED_TYPES.contains(type);
  }
}

