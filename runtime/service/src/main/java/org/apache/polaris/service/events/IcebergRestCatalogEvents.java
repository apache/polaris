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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * Event records for Iceberg REST Catalog operations. Each operation has corresponding "Before" and
 * "After" event records.
 */
public class IcebergRestCatalogEvents {

  // Namespace Events
  public record BeforeCreateNamespaceEvent(
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
      PolarisEventMetadata metadata, String catalogName, String parent) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_NAMESPACES;
    }
  }

  public record AfterListNamespacesEvent(
      PolarisEventMetadata metadata, String catalogName, String parent) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_NAMESPACES;
    }
  }

  public record BeforeLoadNamespaceMetadataEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_NAMESPACE_METADATA;
    }
  }

  public record AfterLoadNamespaceMetadataEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_NAMESPACE;
    }
  }

  public record AfterCheckExistsNamespaceEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_NAMESPACE;
    }
  }

  public record BeforeDropNamespaceEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_NAMESPACE;
    }
  }

  public record AfterDropNamespaceEvent(
      PolarisEventMetadata metadata, String catalogName, String namespace) implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_NAMESPACE;
    }
  }

  public record BeforeUpdateNamespacePropertiesEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_TABLES;
    }
  }

  public record AfterListTablesEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_TABLES;
    }
  }

  public record BeforeLoadTableEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_TABLE;
    }
  }

  public record AfterCheckExistsTableEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_TABLE;
    }
  }

  public record BeforeDropTableEvent(
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
      PolarisEventMetadata metadata, String catalogName, RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_RENAME_TABLE;
    }
  }

  public record AfterRenameTableEvent(
      PolarisEventMetadata metadata, String catalogName, RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_RENAME_TABLE;
    }
  }

  public record BeforeUpdateTableEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceTable,
      UpdateTableRequest commitTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_TABLE;
    }
  }

  /** LoadTableResponse is optional; it will not be populated in case of a transaction. */
  public record AfterUpdateTableEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String sourceTable,
      UpdateTableRequest commitTableRequest,
      LoadTableResponse loadTableResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_TABLE;
    }
  }

  // View Events
  public record BeforeCreateViewEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_VIEWS;
    }
  }

  public record AfterListViewsEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_VIEWS;
    }
  }

  public record BeforeLoadViewEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_VIEW;
    }
  }

  public record AfterLoadViewEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CHECK_EXISTS_VIEW;
    }
  }

  public record AfterCheckExistsViewEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CHECK_EXISTS_VIEW;
    }
  }

  public record BeforeDropViewEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_VIEW;
    }
  }

  public record AfterDropViewEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String view)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_VIEW;
    }
  }

  public record BeforeRenameViewEvent(
      PolarisEventMetadata metadata, String catalogName, RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_RENAME_VIEW;
    }
  }

  public record AfterRenameViewEvent(
      PolarisEventMetadata metadata, String catalogName, RenameTableRequest renameTableRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_RENAME_VIEW;
    }
  }

  public record BeforeReplaceViewEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_CREDENTIALS;
    }
  }

  public record AfterLoadCredentialsEvent(
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_CREDENTIALS;
    }
  }

  // Transaction Events
  public record BeforeCommitTransactionEvent(
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
      PolarisEventMetadata metadata, String catalogName, Namespace namespace, String table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_SEND_NOTIFICATION;
    }
  }

  // Configuration Events
  public record BeforeGetConfigEvent(PolarisEventMetadata metadata, String warehouse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_CONFIG;
    }
  }

  public record AfterGetConfigEvent(PolarisEventMetadata metadata, ConfigResponse configResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_CONFIG;
    }
  }

  // Legacy events
  public record BeforeCommitViewEvent(
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
      PolarisEventMetadata metadata, String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REFRESH_TABLE;
    }
  }

  public record AfterRefreshTableEvent(
      PolarisEventMetadata metadata, String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REFRESH_TABLE;
    }
  }

  public record BeforeRefreshViewEvent(
      PolarisEventMetadata metadata, String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REFRESH_VIEW;
    }
  }

  public record AfterRefreshViewEvent(
      PolarisEventMetadata metadata, String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REFRESH_VIEW;
    }
  }

  // Metrics Reporting Events

  /**
   * Event emitted before a metrics report is processed.
   *
   * @param metadata Event metadata including timestamp, realm, user, and request context
   * @param catalogName The name of the catalog
   * @param namespace The namespace containing the table
   * @param table The table name for which metrics are being reported
   * @param reportMetricsRequest The metrics report request (ScanReport or CommitReport)
   */
  public record BeforeReportMetricsEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_REPORT_METRICS;
    }
  }

  /**
   * Event emitted after a metrics report has been processed.
   *
   * <p>This event enables audit logging of compute engine metrics reports, including scan metrics
   * (files scanned, bytes read, planning duration) and commit metrics (files added/removed,
   * operation type). The metadata map in the report can contain trace context for correlation with
   * other audit events.
   *
   * @param metadata Event metadata including timestamp, realm, user, and request context
   * @param catalogName The name of the catalog
   * @param namespace The namespace containing the table
   * @param table The table name for which metrics were reported
   * @param reportMetricsRequest The metrics report request (ScanReport or CommitReport)
   */
  public record AfterReportMetricsEvent(
      PolarisEventMetadata metadata,
      String catalogName,
      Namespace namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_REPORT_METRICS;
    }
  }
}
