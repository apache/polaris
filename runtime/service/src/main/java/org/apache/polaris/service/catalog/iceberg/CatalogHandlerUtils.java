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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdate.UpgradeFormatVersion;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CODE_COPIED_TO_POLARIS Copied from CatalogHandler in Iceberg 1.8.0 Contains a collection of
 * utilities related to managing Iceberg entities
 */
@ApplicationScoped
public class CatalogHandlerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogHandlerUtils.class);

  private static final Schema EMPTY_SCHEMA = new Schema();
  private static final String INITIAL_PAGE_TOKEN = "";
  private static final String CONFLICT_RESOLUTION_ACTION =
      "polaris.internal.conflict-resolution.by-operation-type.replace";
  private static final Field LAST_SEQUENCE_NUMBER_FIELD;

  static {
    try {
      LAST_SEQUENCE_NUMBER_FIELD =
          TableMetadata.Builder.class.getDeclaredField("lastSequenceNumber");
      LAST_SEQUENCE_NUMBER_FIELD.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Unable to access field", e);
    }
  }

  private final int maxCommitRetries;
  private final boolean rollbackCompactionEnabled;

  @Inject
  public CatalogHandlerUtils(RealmConfig realmConfig) {
    this(
        realmConfig.getConfig(FeatureConfiguration.ICEBERG_COMMIT_MAX_RETRIES),
        realmConfig.getConfig(FeatureConfiguration.ICEBERG_ROLLBACK_COMPACTION_ON_CONFLICTS));
  }

  @VisibleForTesting
  public CatalogHandlerUtils(int maxCommitRetries, boolean rollbackCompactionEnabled) {
    this.maxCommitRetries = maxCommitRetries;
    this.rollbackCompactionEnabled = rollbackCompactionEnabled;
  }

  /**
   * Exception used to avoid retrying commits when assertions fail.
   *
   * <p>When a REST assertion fails, it will throw CommitFailedException to send back to the client.
   * But the assertion checks happen in the block that is retried if {@link
   * TableOperations#commit(TableMetadata, TableMetadata)} throws CommitFailedException. This is
   * used to avoid retries for assertion failures, which are unwrapped and rethrown outside of the
   * commit loop.
   */
  private static class ValidationFailureException extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailureException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }

  private <T> Pair<List<T>, String> paginate(
      List<T> list, @Nullable String pageToken, @Nullable Integer pageSize) {
    if (pageToken == null) {
      return Pair.of(list, null);
    }

    int pageStart = INITIAL_PAGE_TOKEN.equals(pageToken) ? 0 : Integer.parseInt(pageToken);
    if (pageStart >= list.size()) {
      return Pair.of(Collections.emptyList(), null);
    }

    // if pageSize is null, return the rest of the list
    pageSize = pageSize == null ? list.size() : pageSize;
    int end = Math.min(pageStart + pageSize, list.size());
    List<T> subList = list.subList(pageStart, end);
    String nextPageToken = end >= list.size() ? null : String.valueOf(end);

    return Pair.of(subList, nextPageToken);
  }

  public ListNamespacesResponse listNamespaces(SupportsNamespaces catalog, Namespace parent) {
    List<Namespace> results;
    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    return ListNamespacesResponse.builder().addAll(results).build();
  }

  public ListNamespacesResponse listNamespaces(
      SupportsNamespaces catalog, Namespace parent, String pageToken, Integer pageSize) {
    List<Namespace> results;

    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    Pair<List<Namespace>, String> page = paginate(results, pageToken, pageSize);

    return ListNamespacesResponse.builder()
        .addAll(page.first())
        .nextPageToken(page.second())
        .build();
  }

  public CreateNamespaceResponse createNamespace(
      SupportsNamespaces catalog, CreateNamespaceRequest request) {
    Namespace namespace = request.namespace();
    catalog.createNamespace(namespace, request.properties());
    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(catalog.loadNamespaceMetadata(namespace))
        .build();
  }

  public void namespaceExists(SupportsNamespaces catalog, Namespace namespace) {
    if (!catalog.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public GetNamespaceResponse loadNamespace(SupportsNamespaces catalog, Namespace namespace) {
    Map<String, String> properties = catalog.loadNamespaceMetadata(namespace);
    return GetNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(properties)
        .build();
  }

  public void dropNamespace(SupportsNamespaces catalog, Namespace namespace) {
    boolean dropped = catalog.dropNamespace(namespace);
    if (!dropped) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      SupportsNamespaces catalog, Namespace namespace, UpdateNamespacePropertiesRequest request) {
    request.validate();

    Set<String> removals = Sets.newHashSet(request.removals());
    Map<String, String> updates = request.updates();

    Map<String, String> startProperties = catalog.loadNamespaceMetadata(namespace);
    Set<String> missing = Sets.difference(removals, startProperties.keySet());

    if (!updates.isEmpty()) {
      catalog.setProperties(namespace, updates);
    }

    if (!removals.isEmpty()) {
      // remove the original set just in case there was an update just after loading properties
      catalog.removeProperties(namespace, removals);
    }

    return UpdateNamespacePropertiesResponse.builder()
        .addMissing(missing)
        .addUpdated(updates.keySet())
        .addRemoved(Sets.difference(removals, missing))
        .build();
  }

  public ListTablesResponse listTables(Catalog catalog, Namespace namespace) {
    List<TableIdentifier> idents = catalog.listTables(namespace);
    return ListTablesResponse.builder().addAll(idents).build();
  }

  public ListTablesResponse listTables(
      Catalog catalog, Namespace namespace, String pageToken, Integer pageSize) {
    List<TableIdentifier> results = catalog.listTables(namespace);

    Pair<List<TableIdentifier>, String> page = paginate(results, pageToken, pageSize);

    return ListTablesResponse.builder().addAll(page.first()).nextPageToken(page.second()).build();
  }

  public LoadTableResponse stageTableCreate(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (catalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    String location;
    if (request.location() != null) {
      location = request.location();
    } else {
      location =
          catalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);

    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  public LoadTableResponse createTable(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table =
        catalog
            .buildTable(ident, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(request.properties())
            .create();

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public LoadTableResponse registerTable(
      Catalog catalog, Namespace namespace, RegisterTableRequest request) {
    request.validate();

    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    Table table = catalog.registerTable(identifier, request.metadataLocation());
    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public void dropTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, false);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public void purgeTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, true);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public void tableExists(Catalog catalog, TableIdentifier ident) {
    boolean exists = catalog.tableExists(ident);
    if (!exists) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public LoadTableResponse loadTable(Catalog catalog, TableIdentifier ident) {
    Table table = catalog.loadTable(ident);

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public LoadTableResponse updateTable(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    TableMetadata finalMetadata;
    if (isCreate(request)) {
      // this is a hacky way to get TableOperations for an uncommitted table
      Transaction transaction =
          catalog.buildTable(ident, EMPTY_SCHEMA).createOrReplaceTransaction();
      if (transaction instanceof BaseTransaction) {
        BaseTransaction baseTransaction = (BaseTransaction) transaction;
        finalMetadata = create(baseTransaction.underlyingOps(), request);
      } else {
        throw new IllegalStateException(
            "Cannot wrap catalog that does not produce BaseTransaction");
      }

    } else {
      Table table = catalog.loadTable(ident);
      if (table instanceof BaseTable) {
        TableOperations ops = ((BaseTable) table).operations();
        finalMetadata = commit(ops, request);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    return LoadTableResponse.builder().withTableMetadata(finalMetadata).build();
  }

  public void renameTable(Catalog catalog, RenameTableRequest request) {
    catalog.renameTable(request.source(), request.destination());
  }

  private boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private TableMetadata create(TableOperations ops, UpdateTableRequest request) {
    // the only valid requirement is that the table will be created
    request.requirements().forEach(requirement -> requirement.validate(ops.current()));
    Optional<Integer> formatVersion =
        request.updates().stream()
            .filter(update -> update instanceof UpgradeFormatVersion)
            .map(update -> ((UpgradeFormatVersion) update).formatVersion())
            .findFirst();

    TableMetadata.Builder builder =
        formatVersion.map(TableMetadata::buildFromEmpty).orElseGet(TableMetadata::buildFromEmpty);
    request.updates().forEach(update -> update.applyTo(builder));
    // create transactions do not retry. if the table exists, retrying is not a solution
    ops.commit(null, builder.build());

    return ops.current();
  }

  @VisibleForTesting
  public TableMetadata commit(TableOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(maxCommitRetries)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();

                TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
                TableMetadata newBase = base;
                try {
                  request.requirements().forEach((requirement) -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  if (!rollbackCompactionEnabled) {
                    // wrap and rethrow outside of tasks to avoid unnecessary retry
                    throw new ValidationFailureException(e);
                  }
                  LOGGER.debug(
                      "Attempting to Rollback replace operations for table={}, with current-snapshot-id={}",
                      base.uuid(),
                      base.currentSnapshot().snapshotId());
                  UpdateRequirement.AssertRefSnapshotID assertRefSnapshotId =
                      findAssertRefSnapshotID(request);
                  MetadataUpdate.SetSnapshotRef setSnapshotRef = findSetSnapshotRefUpdate(request);

                  if (assertRefSnapshotId == null || setSnapshotRef == null) {
                    // This implies the request was not trying to add a snapshot.
                    LOGGER.debug(
                        "Giving up on Rollback replace operations for table={}, with current-snapshot-id={}, as operation doesn't attempts to add a single snapshot",
                        base.uuid(),
                        base.currentSnapshot().snapshotId());
                    // wrap and rethrow outside of tasks to avoid unnecessary retry
                    throw new ValidationFailureException(e);
                  }

                  // snapshot-id the client expects the table current_snapshot_id
                  long expectedCurrentSnapshotId = assertRefSnapshotId.snapshotId();

                  MetadataUpdate.AddSnapshot snapshotToBeAdded = findAddSnapshotUpdate(request);
                  if (snapshotToBeAdded == null) {
                    // Re-throw if, there's no snapshot data to be added.
                    // wrap and rethrow outside of tasks to avoid unnecessary retry
                    throw new ValidationFailureException(e);
                  }

                  LOGGER.info(
                      "Attempting to Rollback replace operation for table={}, with current-snapshot-id={}, to snapshot={}",
                      base.uuid(),
                      base.currentSnapshot().snapshotId(),
                      snapshotToBeAdded.snapshot().snapshotId());

                  List<MetadataUpdate> metadataUpdates =
                      generateUpdatesToRemoveNoopSnapshot(
                          base, expectedCurrentSnapshotId, setSnapshotRef.name());

                  if (metadataUpdates == null || metadataUpdates.isEmpty()) {
                    // Nothing can be done as this implies that there were not all
                    // No-op snapshots (REPLACE) between expectedCurrentSnapshotId and
                    // currentSnapshotId. hence re-throw the exception caught.
                    // wrap and rethrow outside of tasks to avoid unnecessary retry
                    throw new ValidationFailureException(e);
                  }
                  // Set back the ref we wanted to set, back to the snapshot-id
                  // the client is expecting the table to be at.
                  metadataBuilder.setBranchSnapshot(
                      expectedCurrentSnapshotId, setSnapshotRef.name());

                  // apply the remove snapshots update in the current metadata.
                  // NOTE: we need to setRef to expectedCurrentSnapshotId first and then apply
                  // remove, as otherwise the remove will drop the reference.
                  // NOTE: we can skip removing the now orphan base. It's not a hard requirement.
                  // just something good to do, and not leave for Remove Orphans.
                  // Ref rolled back update correctly to snapshot to be committed parent now.
                  metadataUpdates.forEach((update -> update.applyTo(metadataBuilder)));
                  newBase =
                      setAppropriateLastSeqNumber(
                              metadataBuilder,
                              base.uuid(),
                              base.lastSequenceNumber(),
                              base.snapshot(expectedCurrentSnapshotId).sequenceNumber())
                          .build();
                  LOGGER.info(
                      "Successfully roll-backed replace operation for table={}, with current-snapshot-id={}, to snapshot={}",
                      base.uuid(),
                      base.currentSnapshot().snapshotId(),
                      newBase.currentSnapshot().snapshotId());
                }
                // double check if the requirements passes now.
                try {
                  TableMetadata baseWithRemovedSnaps = newBase;
                  request
                      .requirements()
                      .forEach((requirement) -> requirement.validate(baseWithRemovedSnaps));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(newBase);
                request.updates().forEach((update) -> update.applyTo(newMetadataBuilder));
                TableMetadata updated = newMetadataBuilder.build();
                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }
                taskOps.commit(base, updated);
              });
    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }

  private UpdateRequirement.AssertRefSnapshotID findAssertRefSnapshotID(
      UpdateTableRequest request) {
    UpdateRequirement.AssertRefSnapshotID assertRefSnapshotID = null;
    int total = 0;
    for (UpdateRequirement requirement : request.requirements()) {
      if (requirement instanceof UpdateRequirement.AssertRefSnapshotID) {
        ++total;
        assertRefSnapshotID = (UpdateRequirement.AssertRefSnapshotID) requirement;
      }
    }

    // if > 1 assertion for refs, then it's not safe to roll back, make this Noop.
    return total != 1 ? null : assertRefSnapshotID;
  }

  private List<MetadataUpdate> generateUpdatesToRemoveNoopSnapshot(
      TableMetadata base, long expectedCurrentSnapshotId, String updateRefName) {
    // find the all the snapshots we want to retain which are not the part of current branch.
    Set<Long> idsToRetain = Sets.newHashSet();
    for (Map.Entry<String, SnapshotRef> ref : base.refs().entrySet()) {
      String refName = ref.getKey();
      SnapshotRef snapshotRef = ref.getValue();
      if (refName.equals(updateRefName)) {
        continue;
      }
      idsToRetain.add(ref.getValue().snapshotId());
      // Always check the ancestry for both branch and tags
      // mostly for case where a branch was created and then was dropped
      // then a tag was created and then rollback happened post that tag
      // was dropped and branch was re-created on it.
      for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshotRef.snapshotId(), base::snapshot)) {
        idsToRetain.add(ancestor.snapshotId());
      }
    }

    List<MetadataUpdate> updateToRemoveSnapshot = new ArrayList<>();
    Long snapshotId = base.ref(updateRefName).snapshotId(); // current tip of the given branch
    // ensure this branch has the latest sequence number.
    long expectedSequenceNumber = base.lastSequenceNumber();
    // Unexpected state as table's current sequence number is not equal to the
    // most recent snapshot the ref points to.
    if (expectedSequenceNumber != base.snapshot(snapshotId).sequenceNumber()) {
      LOGGER.debug(
          "Giving up rolling back table {} to snapshot {}, ref current snapshot sequence number {} is not equal expected sequence number {}",
          base.uuid(),
          snapshotId,
          base.snapshot(snapshotId).sequenceNumber(),
          expectedSequenceNumber);
      return null;
    }
    Set<Long> snapshotsToRemove = new LinkedHashSet<>();
    while (snapshotId != null && !Objects.equals(snapshotId, expectedCurrentSnapshotId)) {
      Snapshot snap = base.snapshot(snapshotId);
      if (!isRollbackSnapshot(snap) || idsToRetain.contains(snapshotId)) {
        // Either encountered a non no-op snapshot or the snapshot is being referenced by any other
        // reference either by branch or a tag.
        LOGGER.debug(
            "Giving up rolling back table {} to snapshot {}, snapshot to be removed referenced by another branch or tag ancestor",
            base.uuid(),
            snapshotId);
        break;
      }
      snapshotsToRemove.add(snap.snapshotId());
      snapshotId = snap.parentId();
    }

    boolean wasExpectedSnapshotReached = Objects.equals(snapshotId, expectedCurrentSnapshotId);
    updateToRemoveSnapshot.add(new MetadataUpdate.RemoveSnapshots(snapshotsToRemove));
    return wasExpectedSnapshotReached ? updateToRemoveSnapshot : null;
  }

  private boolean isRollbackSnapshot(Snapshot snapshot) {
    // Only Snapshots with {@ROLLBACKABLE_REPLACE_SNAPSHOT} are allowed to be rollback.
    return DataOperations.REPLACE.equals(snapshot.operation())
        && PropertyUtil.propertyAsString(snapshot.summary(), CONFLICT_RESOLUTION_ACTION, "")
            .equalsIgnoreCase("rollback");
  }

  private MetadataUpdate.SetSnapshotRef findSetSnapshotRefUpdate(UpdateTableRequest request) {
    int total = 0;
    MetadataUpdate.SetSnapshotRef setSnapshotRefUpdate = null;
    // find the SetRefName snapshot update
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.SetSnapshotRef) {
        total++;
        setSnapshotRefUpdate = (MetadataUpdate.SetSnapshotRef) update;
      }
    }

    // if > 1 assertion for refs, then it's not safe to rollback, make this Noop.
    return total != 1 ? null : setSnapshotRefUpdate;
  }

  private MetadataUpdate.AddSnapshot findAddSnapshotUpdate(UpdateTableRequest request) {
    int total = 0;
    MetadataUpdate.AddSnapshot addSnapshot = null;
    // find the SetRefName snapshot update
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.AddSnapshot) {
        total++;
        addSnapshot = (MetadataUpdate.AddSnapshot) update;
      }
    }

    // if > 1 assertion for addSnapshot, then it's not safe to rollback, make this Noop.
    return total != 1 ? null : addSnapshot;
  }

  private TableMetadata.Builder setAppropriateLastSeqNumber(
      TableMetadata.Builder metadataBuilder,
      String tableUUID,
      long currentSequenceNumber,
      long expectedSequenceNumber) {
    // TODO: Get rid of the reflection call once TableMetadata have API for it.
    // move the lastSequenceNumber back, to apply snapshot properly on the
    // current-metadata Seq number are considered increasing monotonically
    // snapshot over snapshot, the client generates the manifest list and hence
    // the sequence number can't be changed for a snapshot the only possible option
    // then is to change the sequenceNumber tracked by metadata.json
    try {
      // this should point to the sequence number that current tip of the
      // branch belongs to, as the new commit will be applied on top of this.
      LAST_SEQUENCE_NUMBER_FIELD.set(metadataBuilder, expectedSequenceNumber);
      LOGGER.info(
          "Setting table uuid:{} last sequence number from:{} to {}",
          tableUUID,
          currentSequenceNumber,
          expectedSequenceNumber);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    return metadataBuilder;
  }

  private BaseView asBaseView(View view) {
    Preconditions.checkState(
        view instanceof BaseView, "Cannot wrap catalog that does not produce BaseView");
    return (BaseView) view;
  }

  public ListTablesResponse listViews(ViewCatalog catalog, Namespace namespace) {
    return ListTablesResponse.builder().addAll(catalog.listViews(namespace)).build();
  }

  public ListTablesResponse listViews(
      ViewCatalog catalog, Namespace namespace, String pageToken, Integer pageSize) {
    List<TableIdentifier> results = catalog.listViews(namespace);

    Pair<List<TableIdentifier>, String> page = paginate(results, pageToken, pageSize);

    return ListTablesResponse.builder().addAll(page.first()).nextPageToken(page.second()).build();
  }

  public LoadViewResponse createView(
      ViewCatalog catalog, Namespace namespace, CreateViewRequest request) {
    request.validate();

    ViewBuilder viewBuilder =
        catalog
            .buildView(TableIdentifier.of(namespace, request.name()))
            .withSchema(request.schema())
            .withProperties(request.properties())
            .withDefaultNamespace(request.viewVersion().defaultNamespace())
            .withDefaultCatalog(request.viewVersion().defaultCatalog())
            .withLocation(request.location());

    Set<String> unsupportedRepresentations =
        request.viewVersion().representations().stream()
            .filter(r -> !(r instanceof SQLViewRepresentation))
            .map(ViewRepresentation::type)
            .collect(Collectors.toSet());

    if (!unsupportedRepresentations.isEmpty()) {
      throw new IllegalStateException(
          String.format("Found unsupported view representations: %s", unsupportedRepresentations));
    }

    request.viewVersion().representations().stream()
        .filter(SQLViewRepresentation.class::isInstance)
        .map(SQLViewRepresentation.class::cast)
        .forEach(r -> viewBuilder.withQuery(r.dialect(), r.sql()));

    View view = viewBuilder.create();

    return viewResponse(view);
  }

  private LoadViewResponse viewResponse(View view) {
    ViewMetadata metadata = asBaseView(view).operations().current();
    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(metadata.metadataFileLocation())
        .build();
  }

  public void viewExists(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    if (!catalog.viewExists(viewIdentifier)) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }
  }

  public LoadViewResponse loadView(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    View view = catalog.loadView(viewIdentifier);
    return viewResponse(view);
  }

  public LoadViewResponse updateView(
      ViewCatalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    View view = catalog.loadView(ident);
    ViewMetadata metadata = commit(asBaseView(view).operations(), request);

    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(metadata.metadataFileLocation())
        .build();
  }

  public void renameView(ViewCatalog catalog, RenameTableRequest request) {
    catalog.renameView(request.source(), request.destination());
  }

  public void dropView(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    boolean dropped = catalog.dropView(viewIdentifier);
    if (!dropped) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }
  }

  protected ViewMetadata commit(ViewOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(maxCommitRetries)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                ViewMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // validate requirements
                try {
                  request.requirements().forEach(requirement -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                // apply changes
                ViewMetadata.Builder metadataBuilder = ViewMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(metadataBuilder));

                ViewMetadata updated = metadataBuilder.build();

                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }

                // commit
                taskOps.commit(base, updated);
              });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }
}
