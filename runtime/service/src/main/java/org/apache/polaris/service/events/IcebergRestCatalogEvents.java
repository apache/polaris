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

import java.util.Map;
import java.util.UUID;
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
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * Event records for Iceberg REST Catalog operations. Each operation has corresponding "Before" and
 * "After" event records.
 */
public class IcebergRestCatalogEvents {

  // Namespace Events
  public record BeforeCreateNamespaceEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      CreateNamespaceRequest createNamespaceRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_NAMESPACE;
    }
  }

  public record AfterCreateNamespaceEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      Map<String, String> namespaceProperties)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_NAMESPACE;
    }
  }

  public record BeforeListNamespacesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String parent)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_NAMESPACES;
    }
  }

  public record AfterListNamespacesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String parent)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_NAMESPACES;
    }
  }

  public record BeforeLoadNamespaceMetadataEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_NAMESPACE_METADATA;
    }
  }

  public record AfterLoadNamespaceMetadataEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      Map<String, String> namespaceProperties)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_NAMESPACE_METADATA;
    }
  }

  public record BeforeCheckExistsNamespaceEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_NAMESPACE;
    }
  }

  public record AfterCheckExistsNamespaceEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_NAMESPACE;
    }
  }

  public record BeforeDropNamespaceEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_NAMESPACE;
    }
  }

  public record AfterDropNamespaceEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_NAMESPACE;
    }
  }

  public record BeforeUpdateNamespacePropertiesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_NAMESPACE_PROPERTIES;
    }
  }

  public record AfterUpdateNamespacePropertiesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_NAMESPACE_PROPERTIES;
    }
  }

  // Table Events
  public record BeforeCreateTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_TABLE;
    }
  }

  public record AfterCreateTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String tableName,
      LoadTableResponse loadTableResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_TABLE;
    }
  }

  public record BeforeListTablesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_TABLES;
    }
  }

  public record AfterListTablesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_TABLES;
    }
  }

  public record BeforeLoadTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      String accessDelegationMode,
      String ifNoneMatchString,
      String snapshots)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_TABLE;
    }
  }

  public record AfterLoadTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String tableName,
      LoadTableResponse loadTableResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_TABLE;
    }
  }

  public record BeforeCheckExistsTableEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_TABLE;
    }
  }

  public record AfterCheckExistsTableEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_TABLE;
    }
  }

  public record BeforeDropTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      Boolean purgeRequested)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_TABLE;
    }
  }

  public record AfterDropTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      Boolean purgeRequested)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_TABLE;
    }
  }

  public record BeforeRegisterTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      RegisterTableRequest registerTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REGISTER_TABLE;
    }
  }

  public record AfterRegisterTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String tableName,
      LoadTableResponse loadTableResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REGISTER_TABLE;
    }
  }

  public record BeforeRenameTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_RENAME_TABLE;
    }
  }

  public record AfterRenameTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_RENAME_TABLE;
    }
  }

  public record BeforeUpdateTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceTable,
      CommitTableRequest commitTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_TABLE;
    }
  }

  public record AfterUpdateTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceTable,
      CommitTableRequest commitTableRequest,
      LoadTableResponse loadTableResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_TABLE;
    }
  }

  // View Events
  public record BeforeCreateViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      CreateViewRequest createViewRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_VIEW;
    }
  }

  public record AfterCreateViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String viewName,
      LoadViewResponse loadViewResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_VIEW;
    }
  }

  public record BeforeListViewsEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_VIEWS;
    }
  }

  public record AfterListViewsEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_VIEWS;
    }
  }

  public record BeforeLoadViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_VIEW;
    }
  }

  public record AfterLoadViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String viewName,
      LoadViewResponse loadViewResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_VIEW;
    }
  }

  public record BeforeCheckExistsViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_VIEW;
    }
  }

  public record AfterCheckExistsViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_VIEW;
    }
  }

  public record BeforeDropViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_VIEW;
    }
  }

  public record AfterDropViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_VIEW;
    }
  }

  public record BeforeRenameViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_RENAME_VIEW;
    }
  }

  public record AfterRenameViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_RENAME_VIEW;
    }
  }

  public record BeforeReplaceViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceView,
      CommitViewRequest commitViewRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REPLACE_VIEW;
    }
  }

  public record AfterReplaceViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceView,
      CommitViewRequest commitViewRequest,
      LoadViewResponse loadViewResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REPLACE_VIEW;
    }
  }

  // Credential Events
  public record BeforeLoadCredentialsEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_CREDENTIALS;
    }
  }

  public record AfterLoadCredentialsEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_CREDENTIALS;
    }
  }

  // Transaction Events
  public record BeforeCommitTransactionEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      CommitTransactionRequest commitTransactionRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_COMMIT_TRANSACTION;
    }
  }

  // TODO: Add all PolarisEntities that were modified with this transaction.
  public record AfterCommitTransactionEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      CommitTransactionRequest commitTransactionRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_COMMIT_TRANSACTION;
    }
  }

  // Notification Events
  public record BeforeSendNotificationEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      NotificationRequest notificationRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_SEND_NOTIFICATION;
    }
  }

  // TODO: Add result once SendNotification API changes are confirmed to return the result.
  public record AfterSendNotificationEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_SEND_NOTIFICATION;
    }
  }

  // Configuration Events
  public record BeforeGetConfigEvent(UUID id, PolarisEventMetadata metadata, String warehouse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CONFIG;
    }
  }

  public record AfterGetConfigEvent(
      UUID id, PolarisEventMetadata metadata, ConfigResponse configResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CONFIG;
    }
  }

  // Legacy events
  public record BeforeCommitTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      TableIdentifier identifier,
      TableMetadata metadataBefore,
      TableMetadata metadataAfter)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_COMMIT_TABLE;
    }
  }

  public record AfterCommitTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      TableIdentifier identifier,
      TableMetadata metadataBefore,
      TableMetadata metadataAfter)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_COMMIT_TABLE;
    }
  }

  public record BeforeCommitViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      TableIdentifier identifier,
      ViewMetadata metadataBefore,
      ViewMetadata metadataAfter)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_COMMIT_VIEW;
    }
  }

  public record AfterCommitViewEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      TableIdentifier identifier,
      ViewMetadata metadataBefore,
      ViewMetadata metadataAfter)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_COMMIT_VIEW;
    }
  }

  public record BeforeRefreshTableEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REFRESH_TABLE;
    }
  }

  public record AfterRefreshTableEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REFRESH_TABLE;
    }
  }

  public record BeforeRefreshViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REFRESH_VIEW;
    }
  }

  public record AfterRefreshViewEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REFRESH_VIEW;
    }
  }
}
