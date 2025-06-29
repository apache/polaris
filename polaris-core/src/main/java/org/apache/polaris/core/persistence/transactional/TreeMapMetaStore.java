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
package org.apache.polaris.core.persistence.transactional;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;

/** Implements a simple in-memory store for Polaris, using tree-map */
public class TreeMapMetaStore {

  /** Slice of data, simple KV store. */
  public class Slice<T> {
    // main KV slice
    private final TreeMap<String, T> slice;

    // if we need to rollback
    private final TreeMap<String, T> undoSlice;

    // the key builder
    private final Function<T, String> buildKey;

    // the key builder
    private final Function<T, T> copyRecord;

    private Slice(Function<T, String> buildKey, Function<T, T> copyRecord) {
      this.slice = new TreeMap<>();
      this.undoSlice = new TreeMap<>();
      this.buildKey = buildKey;
      this.copyRecord = copyRecord;
    }

    public String buildKey(T value) {
      return this.buildKey.apply(value);
    }

    /**
     * read a value in the slice, will return null if not found
     *
     * <p>TODO: return a copy of each object to avoid mutating the records
     *
     * @param key key for that value
     */
    public T read(String key) {
      TreeMapMetaStore.this.ensureReadTr();
      T value = this.slice.getOrDefault(key, null);
      return (value != null) ? this.copyRecord.apply(value) : null;
    }

    /**
     * read a range of values in the slice corresponding to a key prefix
     *
     * @param prefix key prefix
     */
    public List<T> readRange(String prefix) {
      TreeMapMetaStore.this.ensureReadTr();
      if (prefix.isEmpty()) {
        return new ArrayList<>(this.slice.values());
      }

      // end of the key
      String endKey =
          prefix.substring(0, prefix.length() - 1)
              + (char) (prefix.charAt(prefix.length() - 1) + 1);

      // Get the sub-map with keys in the range [prefix, endKey)
      return new ArrayList<>(slice.subMap(prefix, true, endKey, false).values());
    }

    /**
     * write a value in the slice
     *
     * @param value value to write
     */
    public void write(T value) {
      TreeMapMetaStore.this.ensureReadWriteTr();
      T valueToWrite = (value != null) ? this.copyRecord.apply(value) : null;
      String key = this.buildKey(valueToWrite);
      // write undo if needs be
      if (!this.undoSlice.containsKey(key)) {
        this.undoSlice.put(key, this.slice.getOrDefault(key, null));
      }
      this.slice.put(key, valueToWrite);
    }

    /**
     * delete the specified record from the slice
     *
     * @param key key for the record to remove
     */
    public void delete(String key) {
      TreeMapMetaStore.this.ensureReadWriteTr();
      if (slice.containsKey(key)) {
        // write undo if needs be
        if (!this.undoSlice.containsKey(key)) {
          this.undoSlice.put(key, this.slice.getOrDefault(key, null));
        }
        this.slice.remove(key);
      }
    }

    /**
     * delete range of values
     *
     * @param prefix key prefix for the record to remove
     */
    public void deleteRange(String prefix) {
      TreeMapMetaStore.this.ensureReadWriteTr();
      List<T> elements = this.readRange(prefix);
      for (T element : elements) {
        this.delete(element);
      }
    }

    void deleteAll() {
      TreeMapMetaStore.this.ensureReadWriteTr();
      slice.clear();
      undoSlice.clear();
    }

    /**
     * delete the specified record from the slice
     *
     * @param value value to remove
     */
    public void delete(T value) {
      this.delete(this.buildKey(value));
    }

    /** Rollback all changes made to this slice since transaction started */
    private void rollback() {
      TreeMapMetaStore.this.ensureReadWriteTr();
      undoSlice.forEach(
          (key, value) -> {
            if (value == null) {
              slice.remove(key);
            } else {
              slice.put(key, value);
            }
          });
    }

    private void startWriteTransaction() {
      undoSlice.clear();
    }
  }

  /** Transaction on the tree-map store */
  private static class Transaction {
    // if true, we have open a read/write transaction
    private final boolean isWrite;

    /** Constructor */
    private Transaction(boolean isWrite) {
      this.isWrite = isWrite;
    }

    public boolean isWrite() {
      return isWrite;
    }
  }

  // synchronization lock to ensure that only one transaction can be started
  private final Object lock;

  // transaction which was started, will be null if no transaction started
  private Transaction tr;

  // diagnostic services
  private PolarisDiagnostics diagnosticServices;

  // all entities
  private final Slice<PolarisBaseEntity> sliceEntities;

  // all entities by-name
  private final Slice<PolarisBaseEntity> sliceEntitiesActive;

  // all entities just holding their entityVersions and grantVersions
  private final Slice<PolarisBaseEntity> sliceEntitiesChangeTracking;

  // all grant records indexed by securable
  private final Slice<PolarisGrantRecord> sliceGrantRecords;

  // all grant records indexed by grantees
  private final Slice<PolarisGrantRecord> sliceGrantRecordsByGrantee;

  // slice to store principal secrets
  private final Slice<PolarisPrincipalSecrets> slicePrincipalSecrets;

  private final Slice<PolarisPolicyMappingRecord> slicePolicyMappingRecords;

  private final Slice<PolarisPolicyMappingRecord> slicePolicyMappingRecordsByPolicy;

  // next id generator
  private final AtomicLong nextId = new AtomicLong();

  /**
   * Constructor, allocate everything at once
   *
   * @param diagnostics diagnostic services
   */
  public TreeMapMetaStore(@Nonnull PolarisDiagnostics diagnostics) {

    // the entities slice
    this.sliceEntities =
        new Slice<>(
            entity -> String.format("%d::%d", entity.getCatalogId(), entity.getId()),
            entity -> new PolarisBaseEntity.Builder(entity).build());

    // the entities active slice; simply acts as a name-based index into the entities slice
    this.sliceEntitiesActive =
        new Slice<>(
            this::buildEntitiesActiveKey, entity -> new PolarisBaseEntity.Builder(entity).build());

    // change tracking
    this.sliceEntitiesChangeTracking =
        new Slice<>(
            entity -> String.format("%d::%d", entity.getCatalogId(), entity.getId()),
            entity -> new PolarisBaseEntity.Builder(entity).build());

    // grant records by securable
    this.sliceGrantRecords =
        new Slice<>(
            grantRecord ->
                String.format(
                    "%d::%d::%d::%d::%d",
                    grantRecord.getSecurableCatalogId(),
                    grantRecord.getSecurableId(),
                    grantRecord.getGranteeCatalogId(),
                    grantRecord.getGranteeId(),
                    grantRecord.getPrivilegeCode()),
            PolarisGrantRecord::new);

    // grant records by securable
    this.sliceGrantRecordsByGrantee =
        new Slice<>(
            grantRecord ->
                String.format(
                    "%d::%d::%d::%d::%d",
                    grantRecord.getGranteeCatalogId(),
                    grantRecord.getGranteeId(),
                    grantRecord.getSecurableCatalogId(),
                    grantRecord.getSecurableId(),
                    grantRecord.getPrivilegeCode()),
            PolarisGrantRecord::new);

    // principal secrets
    slicePrincipalSecrets =
        new Slice<>(
            principalSecrets -> String.format("%s", principalSecrets.getPrincipalClientId()),
            PolarisPrincipalSecrets::new);

    this.slicePolicyMappingRecords =
        new Slice<>(
            policyMappingRecord ->
                String.format(
                    "%d::%d::%d::%d::%d",
                    policyMappingRecord.getTargetCatalogId(),
                    policyMappingRecord.getTargetId(),
                    policyMappingRecord.getPolicyTypeCode(),
                    policyMappingRecord.getPolicyCatalogId(),
                    policyMappingRecord.getPolicyId()),
            PolarisPolicyMappingRecord::new);

    this.slicePolicyMappingRecordsByPolicy =
        new Slice<>(
            policyMappingRecord ->
                String.format(
                    "%d::%d::%d::%d::%d",
                    policyMappingRecord.getPolicyTypeCode(),
                    policyMappingRecord.getPolicyCatalogId(),
                    policyMappingRecord.getPolicyId(),
                    policyMappingRecord.getTargetCatalogId(),
                    policyMappingRecord.getTargetId()),
            PolarisPolicyMappingRecord::new);

    // no transaction open yet
    this.diagnosticServices = diagnostics;
    this.tr = null;
    this.lock = new Object();
  }

  /**
   * Key for the entities_active slice
   *
   * @param coreEntity core entity
   * @return the key
   */
  String buildEntitiesActiveKey(PolarisEntityCore coreEntity) {
    return String.format(
        "%d::%d::%d::%s",
        coreEntity.getCatalogId(),
        coreEntity.getParentId(),
        coreEntity.getTypeCode(),
        coreEntity.getName());
  }

  /**
   * Key for the entities slice
   *
   * @param coreEntity core entity
   * @return the key
   */
  String buildEntitiesKey(PolarisEntityCore coreEntity) {
    return String.format("%d::%d", coreEntity.getCatalogId(), coreEntity.getId());
  }

  /**
   * Build key from a set of value pairs
   *
   * @param keys string/long/integer values
   * @return unique string identifier
   */
  String buildKeyComposite(Object... keys) {
    StringBuilder result = new StringBuilder();
    for (Object key : keys) {
      if (result.length() != 0) {
        result.append("::");
      }
      result.append(key.toString());
    }
    return result.toString();
  }

  /**
   * Build prefix key from a set of value pairs; prefix key will end with the key separator
   *
   * @param keys string/long/integer values
   * @return unique string identifier
   */
  String buildPrefixKeyComposite(Object... keys) {
    StringBuilder result = new StringBuilder();
    for (Object key : keys) {
      result.append(key.toString());
      result.append("::");
    }
    return result.toString();
  }

  /** Start a read transaction */
  private void startReadTransaction() {
    this.diagnosticServices.check(this.tr == null, "cannot nest transaction");
    this.tr = new Transaction(false);
  }

  /** Start a write transaction */
  private void startWriteTransaction() {
    this.diagnosticServices.check(this.tr == null, "cannot nest transaction");
    this.tr = new Transaction(true);
    this.sliceEntities.startWriteTransaction();
    this.sliceEntitiesActive.startWriteTransaction();
    this.sliceEntitiesChangeTracking.startWriteTransaction();
    this.sliceGrantRecords.startWriteTransaction();
    this.sliceGrantRecordsByGrantee.startWriteTransaction();
    this.slicePrincipalSecrets.startWriteTransaction();
    this.slicePolicyMappingRecords.startWriteTransaction();
    this.slicePolicyMappingRecordsByPolicy.startWriteTransaction();
  }

  /** Rollback transaction */
  void rollback() {
    this.sliceEntities.rollback();
    this.sliceEntitiesActive.rollback();
    this.sliceEntitiesChangeTracking.rollback();
    this.sliceGrantRecords.rollback();
    this.sliceGrantRecordsByGrantee.rollback();
    this.slicePrincipalSecrets.rollback();
    this.slicePolicyMappingRecords.rollback();
    this.slicePolicyMappingRecordsByPolicy.rollback();
  }

  /** Ensure that a read/write FDB transaction has been started */
  public void ensureReadWriteTr() {
    this.diagnosticServices.check(
        this.tr != null && this.tr.isWrite(), "no_write_transaction_started");
  }

  /** Ensure that a read FDB transaction has been started */
  private void ensureReadTr() {
    this.diagnosticServices.checkNotNull(this.tr, "no_read_transaction_started");
  }

  /**
   * Run inside a read/write transaction
   *
   * @param callCtx call context to use
   * @param transactionCode transaction code
   * @return the result of the execution
   */
  public <T> T runInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {

    synchronized (lock) {
      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = callCtx.getDiagServices();
        this.startWriteTransaction();
        return transactionCode.get();
      } catch (Throwable e) {
        if (this.tr != null) {
          this.rollback();
        }
        throw e;
      } finally {
        this.tr = null;
        this.diagnosticServices = null;
      }
    }
  }

  /**
   * Run inside a read/write transaction
   *
   * @param callCtx call context to use
   * @param transactionCode transaction code
   */
  public void runActionInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {

    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = callCtx.getDiagServices();
        this.startWriteTransaction();
        transactionCode.run();
      } catch (Throwable e) {
        if (this.tr != null) {
          this.rollback();
        }
        throw e;
      } finally {
        this.tr = null;
        this.diagnosticServices = null;
      }
    }
  }

  /**
   * Run inside a read only transaction
   *
   * @param callCtx call context to use
   * @param transactionCode transaction code
   * @return the result of the execution
   */
  public <T> T runInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {
    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = callCtx.getDiagServices();
        this.startReadTransaction();
        return transactionCode.get();
      } finally {
        this.tr = null;
        this.diagnosticServices = null;
      }
    }
  }

  /**
   * Run inside a read only transaction
   *
   * @param callCtx call context to use
   * @param transactionCode transaction code
   */
  public void runActionInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {
    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = callCtx.getDiagServices();
        this.startReadTransaction();
        transactionCode.run();
      } finally {
        this.tr = null;
        this.diagnosticServices = null;
      }
    }
  }

  public Slice<PolarisBaseEntity> getSliceEntities() {
    return sliceEntities;
  }

  public Slice<PolarisBaseEntity> getSliceEntitiesActive() {
    return sliceEntitiesActive;
  }

  public Slice<PolarisBaseEntity> getSliceEntitiesChangeTracking() {
    return sliceEntitiesChangeTracking;
  }

  public Slice<PolarisGrantRecord> getSliceGrantRecords() {
    return sliceGrantRecords;
  }

  public Slice<PolarisGrantRecord> getSliceGrantRecordsByGrantee() {
    return sliceGrantRecordsByGrantee;
  }

  public Slice<PolarisPrincipalSecrets> getSlicePrincipalSecrets() {
    return slicePrincipalSecrets;
  }

  public Slice<PolarisPolicyMappingRecord> getSlicePolicyMappingRecords() {
    return slicePolicyMappingRecords;
  }

  public Slice<PolarisPolicyMappingRecord> getSlicePolicyMappingRecordsByPolicy() {
    return slicePolicyMappingRecordsByPolicy;
  }

  /**
   * Next sequence number generator
   *
   * @return next id, must be in a read/write transaction
   */
  public long getNextSequence() {
    this.ensureReadWriteTr();
    return this.nextId.incrementAndGet();
  }

  /** Clear all slices from data */
  void deleteAll() {
    this.ensureReadWriteTr();
    this.sliceEntities.deleteAll();
    this.sliceEntitiesActive.deleteAll();
    this.sliceEntitiesChangeTracking.deleteAll();
    this.sliceGrantRecordsByGrantee.deleteAll();
    this.sliceGrantRecords.deleteAll();
    this.slicePrincipalSecrets.deleteAll();
    this.slicePolicyMappingRecords.deleteAll();
    this.slicePolicyMappingRecordsByPolicy.deleteAll();
  }
}
