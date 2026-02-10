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

import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
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
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
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

/**
 * Standard attribute keys for Polaris events. These keys provide type-safe access to common event
 * attributes and enable automatic pruning/filtering logic.
 */
public final class EventAttributes {
  private EventAttributes() {}

  // Catalog attributes
  public static final AttributeKey<String> CATALOG_NAME =
      new AttributeKey<>("catalog_name", String.class);
  public static final AttributeKey<Catalog> CATALOG = new AttributeKey<>("catalog", Catalog.class);
  public static final AttributeKey<UpdateCatalogRequest> UPDATE_CATALOG_REQUEST =
      new AttributeKey<>("update_catalog_request", UpdateCatalogRequest.class);

  // Namespace attributes
  public static final AttributeKey<Namespace> NAMESPACE =
      new AttributeKey<>("namespace", Namespace.class);
  public static final AttributeKey<String> NAMESPACE_FQN =
      new AttributeKey<>("namespace_fqn", String.class);
  public static final AttributeKey<String> PARENT_NAMESPACE_FQN =
      new AttributeKey<>("parent_namespace_fqn", String.class);
  public static final AttributeKey<CreateNamespaceRequest> CREATE_NAMESPACE_REQUEST =
      new AttributeKey<>("create_namespace_request", CreateNamespaceRequest.class);
  public static final AttributeKey<UpdateNamespacePropertiesRequest>
      UPDATE_NAMESPACE_PROPERTIES_REQUEST =
          new AttributeKey<>(
              "update_namespace_properties_request", UpdateNamespacePropertiesRequest.class);
  public static final AttributeKey<UpdateNamespacePropertiesResponse>
      UPDATE_NAMESPACE_PROPERTIES_RESPONSE =
          new AttributeKey<>(
              "update_namespace_properties_response", UpdateNamespacePropertiesResponse.class);

  public static final AttributeKey<Map<String, String>> NAMESPACE_PROPERTIES =
      new AttributeKey<>("namespace_properties", new TypeToken<>() {});

  // Table attributes
  public static final AttributeKey<String> TABLE_NAME =
      new AttributeKey<>("table_name", String.class);
  public static final AttributeKey<TableIdentifier> TABLE_IDENTIFIER =
      new AttributeKey<>("table_identifier", TableIdentifier.class);
  public static final AttributeKey<CreateTableRequest> CREATE_TABLE_REQUEST =
      new AttributeKey<>("create_table_request", CreateTableRequest.class);
  public static final AttributeKey<UpdateTableRequest> UPDATE_TABLE_REQUEST =
      new AttributeKey<>("update_table_request", UpdateTableRequest.class);
  public static final AttributeKey<RegisterTableRequest> REGISTER_TABLE_REQUEST =
      new AttributeKey<>("register_table_request", RegisterTableRequest.class);
  public static final AttributeKey<RenameTableRequest> RENAME_TABLE_REQUEST =
      new AttributeKey<>("rename_table_request", RenameTableRequest.class);
  public static final AttributeKey<LoadTableResponse> LOAD_TABLE_RESPONSE =
      new AttributeKey<>("load_table_response", LoadTableResponse.class);
  public static final AttributeKey<TableMetadata> TABLE_METADATA =
      new AttributeKey<>("table_metadata", TableMetadata.class);
  // Used internally only. Not for external usage.
  public static final AttributeKey<List<TableMetadata>> TABLE_METADATAS =
      new AttributeKey<>("table_metadatas", new TypeToken<>() {});
  public static final AttributeKey<String> ACCESS_DELEGATION_MODE =
      new AttributeKey<>("access_delegation_mode", String.class);
  public static final AttributeKey<String> IF_NONE_MATCH_STRING =
      new AttributeKey<>("if_none_match_string", String.class);
  public static final AttributeKey<String> SNAPSHOTS =
      new AttributeKey<>("snapshots", String.class);
  public static final AttributeKey<Boolean> PURGE_REQUESTED =
      new AttributeKey<>("purge_requested", Boolean.class);

  // View attributes
  public static final AttributeKey<String> VIEW_NAME =
      new AttributeKey<>("view_name", String.class);
  public static final AttributeKey<TableIdentifier> VIEW_IDENTIFIER =
      new AttributeKey<>("view_identifier", TableIdentifier.class);
  public static final AttributeKey<CreateViewRequest> CREATE_VIEW_REQUEST =
      new AttributeKey<>("create_view_request", CreateViewRequest.class);
  public static final AttributeKey<RegisterViewRequest> REGISTER_VIEW_REQUEST =
      new AttributeKey<>("register_view_request", RegisterViewRequest.class);
  public static final AttributeKey<CommitViewRequest> COMMIT_VIEW_REQUEST =
      new AttributeKey<>("commit_view_request", CommitViewRequest.class);
  public static final AttributeKey<ViewMetadata> VIEW_METADATA_BEFORE =
      new AttributeKey<>("view_metadata_before", ViewMetadata.class);
  public static final AttributeKey<ViewMetadata> VIEW_METADATA_AFTER =
      new AttributeKey<>("view_metadata_after", ViewMetadata.class);
  public static final AttributeKey<LoadViewResponse> LOAD_VIEW_RESPONSE =
      new AttributeKey<>("load_view_response", LoadViewResponse.class);

  // Principal attributes
  public static final AttributeKey<String> PRINCIPAL_NAME =
      new AttributeKey<>("principal_name", String.class);
  public static final AttributeKey<Principal> PRINCIPAL =
      new AttributeKey<>("principal", Principal.class);
  public static final AttributeKey<UpdatePrincipalRequest> UPDATE_PRINCIPAL_REQUEST =
      new AttributeKey<>("update_principal_request", UpdatePrincipalRequest.class);

  // Principal Role attributes
  public static final AttributeKey<String> PRINCIPAL_ROLE_NAME =
      new AttributeKey<>("principal_role_name", String.class);
  public static final AttributeKey<PrincipalRole> PRINCIPAL_ROLE =
      new AttributeKey<>("principal_role", PrincipalRole.class);
  public static final AttributeKey<CreatePrincipalRoleRequest> CREATE_PRINCIPAL_ROLE_REQUEST =
      new AttributeKey<>("create_principal_role_request", CreatePrincipalRoleRequest.class);
  public static final AttributeKey<UpdatePrincipalRoleRequest> UPDATE_PRINCIPAL_ROLE_REQUEST =
      new AttributeKey<>("update_principal_role_request", UpdatePrincipalRoleRequest.class);

  // Catalog Role attributes
  public static final AttributeKey<String> CATALOG_ROLE_NAME =
      new AttributeKey<>("catalog_role_name", String.class);
  public static final AttributeKey<CatalogRole> CATALOG_ROLE =
      new AttributeKey<>("catalog_role", CatalogRole.class);
  public static final AttributeKey<UpdateCatalogRoleRequest> UPDATE_CATALOG_ROLE_REQUEST =
      new AttributeKey<>("update_catalog_role_request", UpdateCatalogRoleRequest.class);

  // Grant attributes
  public static final AttributeKey<AddGrantRequest> ADD_GRANT_REQUEST =
      new AttributeKey<>("add_grant_request", AddGrantRequest.class);
  public static final AttributeKey<RevokeGrantRequest> REVOKE_GRANT_REQUEST =
      new AttributeKey<>("revoke_grant_request", RevokeGrantRequest.class);
  public static final AttributeKey<GrantResource> GRANT_RESOURCE =
      new AttributeKey<>("grant_resource", GrantResource.class);
  public static final AttributeKey<PolarisPrivilege> PRIVILEGE =
      new AttributeKey<>("privilege", PolarisPrivilege.class);
  public static final AttributeKey<Boolean> CASCADE = new AttributeKey<>("cascade", Boolean.class);

  // Transaction attributes
  public static final AttributeKey<CommitTransactionRequest> COMMIT_TRANSACTION_REQUEST =
      new AttributeKey<>("commit_transaction_request", CommitTransactionRequest.class);

  // Notification attributes
  public static final AttributeKey<NotificationRequest> NOTIFICATION_REQUEST =
      new AttributeKey<>("notification_request", NotificationRequest.class);

  // Configuration attributes
  public static final AttributeKey<String> WAREHOUSE =
      new AttributeKey<>("warehouse", String.class);
  public static final AttributeKey<ConfigResponse> CONFIG_RESPONSE =
      new AttributeKey<>("config_response", ConfigResponse.class);

  // Task attributes
  public static final AttributeKey<Long> TASK_ENTITY_ID =
      new AttributeKey<>("task_entity_id", Long.class);
  public static final AttributeKey<Integer> TASK_ATTEMPT =
      new AttributeKey<>("task_attempt", Integer.class);
  public static final AttributeKey<Boolean> TASK_SUCCESS =
      new AttributeKey<>("task_success", Boolean.class);

  // Rate limiting attributes
  public static final AttributeKey<String> HTTP_METHOD =
      new AttributeKey<>("http_method", String.class);
  public static final AttributeKey<String> REQUEST_URI =
      new AttributeKey<>("request_uri", String.class);

  // Generic table attributes
  public static final AttributeKey<String> NAMESPACE_NAME =
      new AttributeKey<>("namespace_name", String.class);
  public static final AttributeKey<String> GENERIC_TABLE_NAME =
      new AttributeKey<>("generic_table_name", String.class);
  public static final AttributeKey<GenericTable> GENERIC_TABLE =
      new AttributeKey<>("generic_table", GenericTable.class);
  public static final AttributeKey<CreateGenericTableRequest> CREATE_GENERIC_TABLE_REQUEST =
      new AttributeKey<>("create_generic_table_request", CreateGenericTableRequest.class);

  // Policy attributes
  public static final AttributeKey<String> POLICY_NAME =
      new AttributeKey<>("policy_name", String.class);
  public static final AttributeKey<String> POLICY_TYPE =
      new AttributeKey<>("policy_type", String.class);
  public static final AttributeKey<String> TARGET_NAME =
      new AttributeKey<>("target_name", String.class);
  public static final AttributeKey<Boolean> DETACH_ALL =
      new AttributeKey<>("detach_all", Boolean.class);
  public static final AttributeKey<CreatePolicyRequest> CREATE_POLICY_REQUEST =
      new AttributeKey<>("create_policy_request", CreatePolicyRequest.class);
  public static final AttributeKey<UpdatePolicyRequest> UPDATE_POLICY_REQUEST =
      new AttributeKey<>("update_policy_request", UpdatePolicyRequest.class);
  public static final AttributeKey<LoadPolicyResponse> LOAD_POLICY_RESPONSE =
      new AttributeKey<>("load_policy_response", LoadPolicyResponse.class);
  public static final AttributeKey<AttachPolicyRequest> ATTACH_POLICY_REQUEST =
      new AttributeKey<>("attach_policy_request", AttachPolicyRequest.class);
  public static final AttributeKey<DetachPolicyRequest> DETACH_POLICY_REQUEST =
      new AttributeKey<>("detach_policy_request", DetachPolicyRequest.class);
  public static final AttributeKey<GetApplicablePoliciesResponse> GET_APPLICABLE_POLICIES_RESPONSE =
      new AttributeKey<>("get_applicable_policies_response", GetApplicablePoliciesResponse.class);
}
