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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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

    // how many values this slice has copied out of its map; see copiedValueCount()
    private long copiedValues;

    // how many values this slice has looked at in its map; see visitedValueCount()
    private long visitedValues;

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
     * @param key key for that value
     */
    public T read(String key) {
      ensureReadTr();
      T value = this.slice.getOrDefault(key, null);
      return (value != null) ? this.copyRecord.apply(value) : null;
    }

    /**
     * read a range of values in the slice corresponding to a key prefix
     *
     * @param prefix key prefix
     */
    public List<T> readRange(String prefix) {
      ensureReadTr();
      return copyValues(rangeValues(prefix));
    }

    /**
     * Read at most {@code limit} values from the range a key prefix names, in key order.
     *
     * <p>The limit bounds the copy itself, not the result of one: a caller that reads the whole
     * range and then stops using it has already paid for every record in it, however few it keeps.
     * That is the difference between a bound on a response and a bound on the work a request does.
     *
     * @param prefix key prefix
     * @param limit the greatest number of values to copy out
     */
    public List<T> readRange(String prefix, int limit) {
      ensureReadTr();
      return copyValues(rangeValues(prefix), limit);
    }

    private Collection<T> rangeValues(String prefix) {
      if (prefix.isEmpty()) {
        return this.slice.values();
      }
      // Get the sub-map with keys in the range [prefix, rangeEndKey(prefix))
      return slice.subMap(prefix, true, rangeEndKey(prefix), false).values();
    }

    /**
     * How many values this slice has copied out of its map since it was created. A bounded read is
     * the only thing that can keep this below the size of the range it was asked for, which is why
     * a test asserts on it: the list a read returns is short either way, so its size proves nothing
     * about the work that produced it.
     */
    @VisibleForTesting
    long copiedValueCount() {
      return this.copiedValues;
    }

    /**
     * How many values this slice has looked at in its map since it was created. It differs from
     * {@link #copiedValueCount()} only for a filtered read: a filter decides per value, so a read
     * that returns one row may have compared many. Keeping the two apart is what lets a test say
     * which of the two a bound actually bounds.
     */
    @VisibleForTesting
    long visitedValueCount() {
      return this.visitedValues;
    }

    /**
     * Reads at most {@code limit} values that {@code filter} accepts, from the range a key prefix
     * names, in key order, starting strictly after {@code afterKeyExclusive} when one is given.
     *
     * <p>Three properties the caller of a paged read needs, and none of them holds for a read that
     * materializes the range first. The map's own order is the read order, so a caller whose key
     * encodes its ordering columns needs no sort. The start key is a seek, so resuming a page costs
     * nothing for the rows already handed out. And the bound is on the copy, so answering one page
     * of a range holding a great many rows copies one page.
     *
     * <p>The bound counts values kept, not values looked at. A filtered read walks until it has
     * filled the bound or reached the end of the range, so an empty result means the range holds
     * nothing else that matches -- which is what a caller minting a continuation from the last row
     * it received has to be able to assume. A filter that may not look at another value says so by
     * throwing, and this read does not catch it: a bound on values looked at is the filter's to
     * enforce and to report.
     *
     * @param prefix key prefix naming the range
     * @param afterKeyExclusive resume strictly after this key, or null to start at the range's
     *     first key
     * @param filter values this read keeps
     * @param limit the greatest number of accepted values to copy out
     */
    public List<T> readRange(
        String prefix, @Nullable String afterKeyExclusive, Predicate<T> filter, int limit) {
      ensureReadTr();
      String start = afterKeyExclusive == null ? prefix : afterKeyExclusive;
      List<T> copied = new ArrayList<>();
      if (limit <= 0) {
        return copied;
      }
      Collection<T> range =
          prefix.isEmpty() && afterKeyExclusive == null
              ? this.slice.values()
              : this.slice
                  .subMap(start, afterKeyExclusive == null, rangeEndKey(prefix), false)
                  .values();
      for (T value : range) {
        this.visitedValues++;
        if (!filter.test(value)) {
          continue;
        }
        copied.add(this.copyRecord.apply(value));
        if (copied.size() >= limit) {
          break;
        }
      }
      this.copiedValues += copied.size();
      return copied;
    }

    private List<T> copyValues(Collection<T> values, int limit) {
      // Not pre-sized from the limit: the caller's unbounded limit is Integer.MAX_VALUE, and not
      // from
      // the range either, because sizing a sub-map view walks it, which is the cost being avoided.
      List<T> copied = new ArrayList<>();
      for (T value : values) {
        if (copied.size() >= limit) {
          break;
        }
        copied.add(this.copyRecord.apply(value));
      }
      this.copiedValues += copied.size();
      this.visitedValues += copied.size();
      return copied;
    }

    private List<T> copyValues(Collection<T> values) {
      List<T> copied = new ArrayList<>(values.size());
      for (T value : values) {
        copied.add(this.copyRecord.apply(value));
      }
      this.copiedValues += copied.size();
      this.visitedValues += copied.size();
      return copied;
    }

    private List<String> copyKeysInRange(String prefix) {
      if (prefix.isEmpty()) {
        return new ArrayList<>(this.slice.keySet());
      }

      return new ArrayList<>(slice.subMap(prefix, true, rangeEndKey(prefix), false).keySet());
    }

    private String rangeEndKey(String prefix) {
      return keyJustPast(prefix);
    }

    /**
     * write a value in the slice
     *
     * @param value value to write
     */
    public void write(T value) {
      ensureReadWriteTr();
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
     * @return whether a record was actually removed; false if the key was not present
     */
    public boolean delete(String key) {
      ensureReadWriteTr();
      boolean existed = slice.containsKey(key);
      if (existed) {
        // write undo if needs be
        if (!this.undoSlice.containsKey(key)) {
          this.undoSlice.put(key, this.slice.getOrDefault(key, null));
        }
        this.slice.remove(key);
      }
      return existed;
    }

    /**
     * delete range of values
     *
     * @param prefix key prefix for the record to remove
     */
    public void deleteRange(String prefix) {
      ensureReadWriteTr();
      List<String> keys = this.copyKeysInRange(prefix);
      for (String key : keys) {
        this.delete(key);
      }
    }

    void deleteAll() {
      ensureReadWriteTr();
      slice.clear();
      undoSlice.clear();
    }

    /**
     * delete the specified record from the slice
     *
     * @param value value to remove
     * @return whether a record was actually removed; false if no matching record was present
     */
    public boolean delete(T value) {
      return this.delete(this.buildKey(value));
    }

    /** Rollback all changes made to this slice since transaction started */
    private void rollback() {
      ensureReadWriteTr();
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

  /**
   * Transaction on the tree-map store
   *
   * @param write if true, we have open a read/write transaction
   */
  private record Transaction(boolean write) {}

  // synchronization lock to ensure that only one transaction can be started
  private final Object lock;

  // transaction which was started, will be null if no transaction started
  private Transaction tr;

  // diagnostic services
  private final PolarisDiagnostics initialDiagnosticServices;
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

  private final Slice<TagAssignmentRecord> sliceTagAssignmentRecords;

  private final Slice<TagAssignmentRecord> sliceTagAssignmentRecordsByTag;

  // next id generator
  private final AtomicLong nextId = new AtomicLong();

  /**
   * Constructor, allocate everything at once
   *
   * @param diagnostics diagnostic services
   */
  public TreeMapMetaStore(@NonNull PolarisDiagnostics diagnostics) {

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

    this.sliceTagAssignmentRecords =
        new Slice<>(
            tagAssignmentRecord ->
                String.format(
                    "%d::%d::%d::%d::%d",
                    tagAssignmentRecord.getTargetCatalogId(),
                    tagAssignmentRecord.getTargetId(),
                    tagAssignmentRecord.getFieldId(),
                    tagAssignmentRecord.getTagCatalogId(),
                    tagAssignmentRecord.getTagId()),
            TagAssignmentRecord::new);

    this.sliceTagAssignmentRecordsByTag =
        new Slice<>(TreeMapMetaStore::buildTagAssignmentByTagKey, TagAssignmentRecord::new);

    this.initialDiagnosticServices = diagnostics;
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

  /** Digits an unsigned 64-bit value takes: {@code 18446744073709551615} is 20 of them. */
  private static final int LONG_KEY_WIDTH = 20;

  /** Digits an unsigned 32-bit value takes: {@code 4294967295} is 10 of them. */
  private static final int INT_KEY_WIDTH = 10;

  /** The separator every composite key uses between its segments. */
  private static final String KEY_SEPARATOR = "::";

  /**
   * One numeric key segment, encoded so that the map's own {@code String} order over the segment is
   * the numeric order of the value it came from.
   *
   * <p>Two things break that agreement and both are fixed here. Printed plainly, {@code 10} sorts
   * before {@code 9} because {@code '1' < '9'}, which a fixed width fixes. And a negative value
   * printed with its sign sorts below every non-negative one but inverts the order among negatives,
   * which flipping the sign bit fixes: {@code value ^ MIN_VALUE} maps the signed range onto the
   * unsigned range in order, so the smallest {@code long} becomes {@code 0} and the largest becomes
   * the largest unsigned value. Encoding both once per write is cheaper than ordering every read,
   * and it turns the map into the index the read wants rather than a bag the read has to sort.
   */
  private static String orderedSegment(long value) {
    return padLeft(Long.toUnsignedString(value ^ Long.MIN_VALUE), LONG_KEY_WIDTH);
  }

  /** {@link #orderedSegment(long)} for a 32-bit segment. */
  private static String orderedSegment(int value) {
    return padLeft(Integer.toUnsignedString(value ^ Integer.MIN_VALUE), INT_KEY_WIDTH);
  }

  private static String padLeft(String digits, int width) {
    return "0".repeat(width - digits.length()) + digits;
  }

  /**
   * The first key that sorts after every key beginning with {@code prefix}: the prefix with its
   * last character stepped by one. A range read uses it as its exclusive upper bound, and a paged
   * read uses it to resume strictly after a row whose full key it cannot rebuild.
   */
  private static String keyJustPast(String prefix) {
    return prefix.substring(0, prefix.length() - 1)
        + (char) (prefix.charAt(prefix.length() - 1) + 1);
  }

  /**
   * Key for the by-tag index of tag assignment records.
   *
   * <p>{@code tagCatalogId::tagId} groups one definition's rows, then {@code targetId::fieldId}
   * orders them, and {@code targetCatalogId} closes the row's stored identity -- the same identity
   * the relational schema declares as this row's primary key. The ordering pair comes before {@code
   * targetCatalogId} so that the order the map iterates is exactly the {@code (targetId, fieldId)}
   * order the reverse-lookup read promises, whatever catalog ids the rows carry: a key that placed
   * {@code targetCatalogId} first would order by it first and would only accidentally agree, in the
   * ordinary case where every target of one definition lives in one catalog.
   */
  static String buildTagAssignmentByTagKey(TagAssignmentRecord record) {
    return buildTagAssignmentByTagOrderedPrefix(
            record.getTagCatalogId(), record.getTagId(), record.getTargetId(), record.getFieldId())
        + orderedSegment(record.getTargetCatalogId());
  }

  /**
   * Prefix naming every assignment row of one tag definition, in {@code (targetId, fieldId)} order.
   */
  static String buildTagAssignmentByTagPrefix(long tagCatalogId, long tagId) {
    return orderedSegment(tagCatalogId) + KEY_SEPARATOR + orderedSegment(tagId) + KEY_SEPARATOR;
  }

  /**
   * Where a page of one definition's assignment rows resumes: strictly after the row a caller last
   * consumed, named by the {@code (targetId, fieldId)} pair its continuation carries. The pair
   * identifies one row of the definition, and the key the row is stored under ends with a segment
   * the continuation does not carry, so the bound is taken just past the pair rather than at the
   * row's own key.
   */
  static String buildTagAssignmentByTagResumeKey(
      long tagCatalogId, long tagId, long targetId, int fieldId) {
    return keyJustPast(
        buildTagAssignmentByTagOrderedPrefix(tagCatalogId, tagId, targetId, fieldId));
  }

  private static String buildTagAssignmentByTagOrderedPrefix(
      long tagCatalogId, long tagId, long targetId, int fieldId) {
    return buildTagAssignmentByTagPrefix(tagCatalogId, tagId)
        + orderedSegment(targetId)
        + KEY_SEPARATOR
        + orderedSegment(fieldId)
        + KEY_SEPARATOR;
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
    this.sliceTagAssignmentRecords.startWriteTransaction();
    this.sliceTagAssignmentRecordsByTag.startWriteTransaction();
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
    this.sliceTagAssignmentRecords.rollback();
    this.sliceTagAssignmentRecordsByTag.rollback();
  }

  /** Ensure that a read/write FDB transaction has been started */
  private void ensureReadWriteTr() {
    this.diagnosticServices.check(
        this.tr != null && this.tr.write(), "no_write_transaction_started");
  }

  /** Ensure that a read FDB transaction has been started */
  private void ensureReadTr() {
    this.diagnosticServices.checkNotNull(this.tr, "no_read_transaction_started");
  }

  /**
   * Run inside a read/write transaction
   *
   * @return the result of the execution
   */
  public <T> T runInTransaction(
      @NonNull PolarisDiagnostics diagnostics, @NonNull Supplier<T> transactionCode) {

    synchronized (lock) {
      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = diagnostics;
        this.startWriteTransaction();
        return transactionCode.get();
      } catch (Throwable e) {
        if (this.tr != null) {
          this.rollback();
        }
        throw e;
      } finally {
        this.tr = null;
        this.diagnosticServices = this.initialDiagnosticServices;
      }
    }
  }

  /** Run inside a read/write transaction */
  public void runActionInTransaction(
      @NonNull PolarisDiagnostics diagnostics, @NonNull Runnable transactionCode) {

    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = diagnostics;
        this.startWriteTransaction();
        transactionCode.run();
      } catch (Throwable e) {
        if (this.tr != null) {
          this.rollback();
        }
        throw e;
      } finally {
        this.tr = null;
        this.diagnosticServices = this.initialDiagnosticServices;
      }
    }
  }

  /**
   * Run inside a read only transaction
   *
   * @return the result of the execution
   */
  public <T> T runInReadTransaction(
      @NonNull PolarisDiagnostics diagnostics, @NonNull Supplier<T> transactionCode) {
    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = diagnostics;
        this.startReadTransaction();
        return transactionCode.get();
      } finally {
        this.tr = null;
        this.diagnosticServices = this.initialDiagnosticServices;
      }
    }
  }

  /** Run inside a read only transaction */
  public void runActionInReadTransaction(
      @NonNull PolarisDiagnostics diagnostics, @NonNull Runnable transactionCode) {
    synchronized (lock) {

      // execute transaction
      try {
        // init diagnostic services
        this.diagnosticServices = diagnostics;
        this.startReadTransaction();
        transactionCode.run();
      } finally {
        this.tr = null;
        this.diagnosticServices = this.initialDiagnosticServices;
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

  public Slice<TagAssignmentRecord> getSliceTagAssignmentRecords() {
    return sliceTagAssignmentRecords;
  }

  public Slice<TagAssignmentRecord> getSliceTagAssignmentRecordsByTag() {
    return sliceTagAssignmentRecordsByTag;
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
    this.sliceTagAssignmentRecords.deleteAll();
    this.sliceTagAssignmentRecordsByTag.deleteAll();
  }
}
