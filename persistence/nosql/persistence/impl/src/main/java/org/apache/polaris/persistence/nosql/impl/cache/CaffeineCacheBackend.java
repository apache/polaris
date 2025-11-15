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
package org.apache.polaris.persistence.nosql.impl.cache;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.api.cache.CacheConfig.DEFAULT_REFERENCE_TTL;
import static org.apache.polaris.persistence.nosql.api.cache.CacheSizing.DEFAULT_CACHE_CAPACITY_OVERSHOOT;
import static org.apache.polaris.persistence.nosql.api.cache.CacheSizing.DEFAULT_HEAP_FRACTION;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.contextualReader;
import static org.apache.polaris.persistence.nosql.api.obj.ObjType.CACHE_UNLIMITED;
import static org.apache.polaris.persistence.nosql.api.obj.ObjType.NOT_CACHED;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.CacheKeyValue.KIND_FLAG_CREATED;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.CacheKeyValue.KIND_FLAG_NUM_PARTS;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.CacheKeyValue.KIND_FLAG_OBJ_ID;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.CacheKeyValue.KIND_FLAG_VERSION;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.CacheKeyValue.KIND_REFERENCE;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.ImmutableCacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CaffeineCacheBackend implements CacheBackend {

  private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineCacheBackend.class);

  public static final String METER_CACHE_CAPACITY = "cache.capacity";
  public static final String METER_CACHE_ADMIT_CAPACITY = "cache.capacity.admitted";
  public static final String METER_CACHE_WEIGHT = "cache.weight-reported";
  public static final String METER_CACHE_REJECTED_WEIGHT = "cache.rejected-weight";

  public static final String CACHE_NAME = "polaris-objects";
  private static final CacheKeyValue NON_EXISTING_SENTINEL =
      new CacheKeyValue("", cacheKeyObjId("CACHE_SENTINEL", 0L, 1, 0L, null), 0L, new byte[0]);

  private final CacheConfig config;
  final Cache<CacheKeyValue, CacheKeyValue> cache;

  private final long refCacheTtlNanos;
  private final long refCacheNegativeTtlNanos;
  private final long capacityBytes;
  private final long admitWeight;
  private final AtomicLong rejections = new AtomicLong();
  private final IntConsumer rejectionsWeight;
  private final LongSupplier weightSupplier;

  private final Lock aboveCapacityLock;

  CaffeineCacheBackend(CacheConfig config, Optional<MeterRegistry> meterRegistry) {
    // Runnable::run as the executor means that eviction runs on a caller thread and is not delayed.
    this(config, meterRegistry, Runnable::run);
  }

  CaffeineCacheBackend(
      CacheConfig config, Optional<MeterRegistry> meterRegistry, Executor executor) {
    this.config = config;

    refCacheTtlNanos = config.referenceTtl().orElse(DEFAULT_REFERENCE_TTL).toNanos();
    refCacheNegativeTtlNanos = config.referenceNegativeTtl().orElse(Duration.ZERO).toNanos();

    var sizing = config.sizing().orElse(ImmutableCacheSizing.builder().build());
    capacityBytes =
        sizing.calculateEffectiveSize(Runtime.getRuntime().maxMemory(), DEFAULT_HEAP_FRACTION);

    admitWeight =
        capacityBytes
            + (long)
                (capacityBytes
                    * sizing.cacheCapacityOvershoot().orElse(DEFAULT_CACHE_CAPACITY_OVERSHOOT));

    var cacheBuilder =
        Caffeine.newBuilder()
            .executor(executor)
            .scheduler(Scheduler.systemScheduler())
            .ticker(config.clockNanos()::getAsLong)
            .maximumWeight(capacityBytes)
            .weigher(this::weigher)
            .expireAfter(
                new Expiry<CacheKeyValue, CacheKeyValue>() {
                  @Override
                  public long expireAfterCreate(
                      @Nonnull CacheKeyValue key,
                      @Nonnull CacheKeyValue value,
                      long currentTimeNanos) {
                    var expire = key.expiresAtNanosEpoch;
                    if (expire == CACHE_UNLIMITED) {
                      return Long.MAX_VALUE;
                    }
                    if (expire == NOT_CACHED) {
                      return 0L;
                    }
                    var remaining = expire - currentTimeNanos;
                    return Math.max(0L, remaining);
                  }

                  @Override
                  public long expireAfterUpdate(
                      @Nonnull CacheKeyValue key,
                      @Nonnull CacheKeyValue value,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return expireAfterCreate(key, value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterRead(
                      @Nonnull CacheKeyValue key,
                      @Nonnull CacheKeyValue value,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return currentDurationNanos;
                  }
                });
    rejectionsWeight =
        meterRegistry
            .map(
                reg -> {
                  cacheBuilder.recordStats(() -> new CaffeineStatsCounter(reg, CACHE_NAME));
                  Gauge.builder(METER_CACHE_CAPACITY, "", x -> capacityBytes)
                      .description("Total capacity of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  Gauge.builder(METER_CACHE_ADMIT_CAPACITY, "", x -> admitWeight)
                      .description("Admitted capacity of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  Gauge.builder(METER_CACHE_WEIGHT, "", x -> (double) currentWeightReported())
                      .description("Current reported weight of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  var rejectedWeightSummary =
                      DistributionSummary.builder(METER_CACHE_REJECTED_WEIGHT)
                          .description("Weight of of rejected cache-puts in bytes.")
                          .tag("cache", CACHE_NAME)
                          .baseUnit(BaseUnits.BYTES)
                          .register(reg);
                  return (IntConsumer) rejectedWeightSummary::record;
                })
            .orElse(x -> {});

    LOGGER.info(
        "Initialized persistence cache with a capacity of ~ {} MB",
        (MemorySize.ofBytes(capacityBytes).asLong() / 1024L / 1024L));

    this.cache = cacheBuilder.build();

    var eviction = cache.policy().eviction().orElseThrow();
    weightSupplier = () -> eviction.weightedSize().orElse(0L);

    aboveCapacityLock = new ReentrantLock();
  }

  @VisibleForTesting
  long currentWeightReported() {
    return weightSupplier.getAsLong();
  }

  @VisibleForTesting
  long rejections() {
    return rejections.get();
  }

  @VisibleForTesting
  long capacityBytes() {
    return capacityBytes;
  }

  @VisibleForTesting
  long admitWeight() {
    return admitWeight;
  }

  @Override
  public Persistence wrap(@Nonnull Persistence persist) {
    return new CachingPersistenceImpl(persist, this);
  }

  private int weigher(CacheKeyValue key, CacheKeyValue value) {
    var size = key.heapSize();
    size += CAFFEINE_OBJ_OVERHEAD;
    return size;
  }

  @Override
  public Obj get(@Nonnull String realmId, @Nonnull ObjRef id) {
    var key = cacheKeyValueObjRead(realmId, id);
    var value = cache.getIfPresent(key);
    if (value == null) {
      return null;
    }
    if (value == NON_EXISTING_SENTINEL) {
      return NOT_FOUND_OBJ_SENTINEL;
    }
    return value.getObj();
  }

  @Override
  public void put(@Nonnull String realmId, @Nonnull Obj obj) {
    putLocal(realmId, obj);
  }

  @VisibleForTesting
  void cachePut(CacheKeyValue key, CacheKeyValue value) {
    var w = weigher(key, value);
    var currentWeight = weightSupplier.getAsLong();
    if (currentWeight < capacityBytes) {
      cache.put(key, value);
      return;
    }

    aboveCapacityLock.lock();
    try {
      cache.cleanUp();
      currentWeight = weightSupplier.getAsLong();
      if (currentWeight + w < admitWeight) {
        cache.put(key, value);
      } else {
        rejections.incrementAndGet();
        rejectionsWeight.accept(w);
      }
    } finally {
      aboveCapacityLock.unlock();
    }
  }

  @Override
  public void putLocal(@Nonnull String realmId, @Nonnull Obj obj) {
    long expiresAt =
        obj.type()
            .cachedObjectExpiresAtMicros(
                obj, () -> NANOSECONDS.toMicros(config.clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      return;
    }

    var expiresAtNanos =
        expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
    var keyValue = cacheKeyValueObj(realmId, obj, expiresAtNanos);
    cachePut(keyValue, keyValue);
  }

  @Override
  public void putNegative(@Nonnull String realmId, @Nonnull ObjRef id) {
    var type = objTypeById(id.type());
    var expiresAt =
        type.negativeCacheExpiresAtMicros(
            () -> NANOSECONDS.toMicros(config.clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      remove(realmId, id);
      return;
    }

    var expiresAtNanos =
        expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
    var keyValue = cacheKeyValueNegative(realmId, cacheKeyObjId(id), expiresAtNanos);

    cachePut(keyValue, NON_EXISTING_SENTINEL);
  }

  @Override
  public void remove(@Nonnull String realmId, @Nonnull ObjRef id) {
    cache.invalidate(cacheKeyValueObjRead(realmId, id));
  }

  @Override
  public void clear(@Nonnull String realmId) {
    cache.asMap().keySet().removeIf(k -> k.realmId.equals(realmId));
  }

  @Override
  public void purge() {
    cache.asMap().clear();
  }

  @Override
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  @Override
  public void removeReference(@Nonnull String realmId, @Nonnull String name) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    cache.invalidate(cacheKeyValueReferenceRead(realmId, name));
  }

  @Override
  public void putReference(@Nonnull String realmId, @Nonnull Reference reference) {
    putReferenceLocal(realmId, reference);
  }

  @Override
  public void putReferenceLocal(@Nonnull String realmId, @Nonnull Reference reference) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    var expiresAtNanos = config.clockNanos().getAsLong() + refCacheTtlNanos;
    var keyValue = cacheKeyValueReference(realmId, reference, expiresAtNanos);
    cachePut(keyValue, keyValue);
  }

  @Override
  public void putReferenceNegative(@Nonnull String realmId, @Nonnull String name) {
    if (refCacheNegativeTtlNanos <= 0L) {
      return;
    }
    var key =
        cacheKeyValueNegative(
            realmId,
            cacheKeyReference(name),
            config.clockNanos().getAsLong() + refCacheNegativeTtlNanos);
    cachePut(key, NON_EXISTING_SENTINEL);
  }

  @Override
  public Reference getReference(@Nonnull String realmId, @Nonnull String name) {
    if (refCacheTtlNanos <= 0L) {
      return null;
    }
    var value = cache.getIfPresent(cacheKeyValueReferenceRead(realmId, name));
    if (value == null) {
      return null;
    }
    if (value == NON_EXISTING_SENTINEL) {
      return NON_EXISTENT_REFERENCE_SENTINEL;
    }
    return value.getReference();
  }

  @VisibleForTesting
  static CacheKeyValue cacheKeyValueObj(
      @Nonnull String realmId, @Nonnull Obj obj, long expiresAtNanos) {
    var serialized = serializeObj(obj);
    return new CacheKeyValue(realmId, cacheKeyObj(obj), expiresAtNanos, serialized);
  }

  @VisibleForTesting
  static CacheKeyValue cacheKeyValueObjRead(@Nonnull String realmId, @Nonnull ObjRef id) {
    return new CacheKeyValue(realmId, cacheKeyObjId(id), 0L, null);
  }

  @VisibleForTesting
  static CacheKeyValue cacheKeyValueReference(
      String realmId, Reference reference, long expiresAtNanos) {
    return new CacheKeyValue(
        realmId,
        cacheKeyReference(reference.name()),
        expiresAtNanos,
        serializeReference(reference));
  }

  @VisibleForTesting
  static CacheKeyValue cacheKeyValueReferenceRead(@Nonnull String realmId, @Nonnull String name) {
    return new CacheKeyValue(realmId, cacheKeyReference(name), 0L, null);
  }

  @VisibleForTesting
  static CacheKeyValue cacheKeyValueNegative(
      @Nonnull String realmId, @Nonnull byte[] key, long expiresAtNanosEpoch) {
    return new CacheKeyValue(realmId, key, expiresAtNanosEpoch, null);
  }

  @VisibleForTesting
  static byte[] cacheKeyObj(@Nonnull Obj obj) {
    return cacheKeyObjId(
        obj.type().id(), obj.id(), obj.numParts(), obj.createdAtMicros(), obj.versionToken());
  }

  @VisibleForTesting
  static byte[] cacheKeyObjId(@Nonnull ObjRef id) {
    return cacheKeyObjId(id.type(), id.id(), id.numParts(), 0L, null);
  }

  private static byte[] cacheKeyObjId(
      String type, long id, int numParts, long createdAtMicros, String versionToken) {
    var typeBytes = type.getBytes(UTF_8);
    var relevantLen = Long.BYTES + typeBytes.length;
    var relevantSerializedLen = varIntLen(relevantLen);
    var versionTokenBytes = versionToken != null ? versionToken.getBytes(UTF_8) : null;

    var keyLen = 1 + relevantSerializedLen + relevantLen;
    var kind = KIND_FLAG_OBJ_ID;
    if (numParts != 1) {
      keyLen += varIntLen(numParts);
      kind |= KIND_FLAG_NUM_PARTS;
    }
    if (createdAtMicros != 0L) {
      keyLen += Long.BYTES;
      kind |= KIND_FLAG_CREATED;
    }
    if (versionTokenBytes != null) {
      keyLen += versionTokenBytes.length;
      kind |= KIND_FLAG_VERSION;
    }

    var key = new byte[keyLen];
    var buf = ByteBuffer.wrap(key);

    buf.put(kind);
    putVarInt(buf, relevantLen);
    buf.putLong(id);
    buf.put(typeBytes);
    if (numParts != 1) {
      putVarInt(buf, numParts);
    }
    if (createdAtMicros != 0L) {
      buf.putLong(createdAtMicros);
    }
    if (versionTokenBytes != null) {
      buf.put(versionTokenBytes);
    }
    return key;
  }

  @VisibleForTesting
  static byte[] cacheKeyReference(String refName) {
    var refNameBytes = refName.getBytes(UTF_8);
    var key = new byte[1 + refNameBytes.length];
    var buf = ByteBuffer.wrap(key);
    buf.put(KIND_REFERENCE);
    buf.put(refNameBytes);
    return key;
  }

  /**
   * Class used for both the cache key and cache value including the expiration timestamp. This is
   * (should be) more efficient (think: monomorphic vs. bi-morphic call sizes) and more GC/heap
   * friendly (less object-instances) than having different object types.
   */
  static final class CacheKeyValue {

    static final byte KIND_REFERENCE = 0;
    static final byte KIND_FLAG_OBJ_ID = 1;
    static final byte KIND_FLAG_NUM_PARTS = 2;
    static final byte KIND_FLAG_CREATED = 4;
    static final byte KIND_FLAG_VERSION = 8;

    final String realmId;
    final byte[] key;
    final int hash;

    // Revisit this field before 2262-04-11T23:47:16.854Z (64-bit signed long overflow) ;) ;)
    final long expiresAtNanosEpoch;

    final byte[] serialized;

    CacheKeyValue(String realmId, byte[] key, long expiresAtNanosEpoch, byte[] serialized) {
      this.realmId = realmId;
      this.key = key;
      this.expiresAtNanosEpoch = expiresAtNanosEpoch;
      this.serialized = serialized;

      var hash = realmId.hashCode();
      var buf = ByteBuffer.wrap(key);
      var kind = buf.get();
      if (kind == KIND_REFERENCE) {
        hash = hash * 31 + Arrays.hashCode(key);
      } else {
        var relevantLen = readVarInt(buf);
        hash = hash * 31 + buf.limit(buf.position() + relevantLen).hashCode();
      }

      this.hash = hash;
    }

    /**
     * Provide a good <em>estimate</em> about the heap usage of this object. The goal of this
     * implementation is to rather yield a potentially higher value than a too low value.
     *
     * <p>The implementation neglects the soft-referenced object, as that can be relatively easily
     * collected by the Java GC.
     */
    int heapSize() {
      var size = CACHE_KEY_VALUE_SIZE;
      // realm id (String)
      size += STRING_SIZE + ARRAY_OVERHEAD + realmId.length();
      // serialized obj-key
      size += ARRAY_OVERHEAD + key.length;
      // serialized value
      byte[] s = serialized;
      if (s != null) {
        size += ARRAY_OVERHEAD + s.length;
      }
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKeyValue that)) {
        return false;
      }
      if (!this.realmId.equals(that.realmId)) {
        return false;
      }

      var thisBuf = ByteBuffer.wrap(this.key);
      var thatBuf = ByteBuffer.wrap(that.key);
      var thisKind = thisBuf.get();
      var thatKind = thatBuf.get();

      if (thisKind == KIND_REFERENCE && thatKind == KIND_REFERENCE) {
        return Arrays.equals(this.key, that.key);
      }
      if (thisKind == KIND_REFERENCE || thatKind == KIND_REFERENCE) {
        return false;
      }

      // must be an object ID
      var thisRelevantLen = readVarInt(thisBuf);
      var thatRelevantLen = readVarInt(thatBuf);
      if (thisRelevantLen != thatRelevantLen) {
        return false;
      }
      var off = thisBuf.position();
      var to = off + thisRelevantLen;
      return Arrays.equals(this.key, off, to, that.key, off, to);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      var sb = new StringBuilder("{");
      sb.append(realmId).append(", ");

      var buf = ByteBuffer.wrap(this.key);
      var kind = buf.get();

      if (kind == KIND_REFERENCE) {
        var referenceName = new String(key, 1, key.length - 1, UTF_8);
        sb.append("reference:").append(referenceName);
      } else {
        var relevantLen = readVarInt(buf);
        var id = buf.getLong();
        var typeIdLen = relevantLen - Long.BYTES;
        var typeId = new String(key, buf.position(), typeIdLen, UTF_8);
        sb.append("obj:").append(typeId).append("/").append(id);
        buf.position(buf.position() + typeIdLen);
        if ((kind & KIND_FLAG_NUM_PARTS) == KIND_FLAG_NUM_PARTS) {
          sb.append(", numParts:").append(readVarInt(buf));
        }
        if ((kind & KIND_FLAG_CREATED) == KIND_FLAG_CREATED) {
          sb.append(", createdAtMicros:").append(buf.getLong());
        }
        if ((kind & KIND_FLAG_VERSION) == KIND_FLAG_VERSION) {
          sb.append(", versionToken:")
              .append(new String(key, buf.position(), key.length - buf.position(), UTF_8));
        }
      }

      return sb.append("}").toString();
    }

    Obj getObj() {
      var buf = ByteBuffer.wrap(this.key);
      var kind = buf.get();
      checkState(kind != KIND_REFERENCE, "Cache value content is not an object");
      var relevantLen = readVarInt(buf);
      var id = buf.getLong();
      var typeIdLen = relevantLen - Long.BYTES;
      var typeId = new String(key, buf.position(), typeIdLen, UTF_8);
      var type = objTypeById(typeId);
      buf.position(buf.position() + typeIdLen);
      var numParts = ((kind & KIND_FLAG_NUM_PARTS) == KIND_FLAG_NUM_PARTS) ? readVarInt(buf) : 1;
      var createdAtMicros = ((kind & KIND_FLAG_CREATED) == KIND_FLAG_CREATED) ? buf.getLong() : 0L;
      var versionToken =
          ((kind & KIND_FLAG_VERSION) == KIND_FLAG_VERSION)
              ? new String(key, buf.position(), key.length - buf.position(), UTF_8)
              : null;

      try {
        return contextualReader(SMILE_MAPPER, type, id, numParts, versionToken, createdAtMicros)
            .readValue(serialized);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    Reference getReference() {
      var kind = key[0];
      checkState(kind == KIND_REFERENCE, "Cache value content is not a reference");
      try {
        return SMILE_MAPPER.readValue(serialized, Reference.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * "Worst" CacheKeyValue heap layout size as reported by {@code jol internals-estimates}. Note:
   * "Lilliput" would bring it down to 32 bytes.
   *
   * <p>Worst layout: <code><pre>
   * ***** Hotspot Layout Simulation (JDK 15, 64-bit model, NO compressed references, NO compressed classes, 8-byte aligned)
   *
   * org.apache.polaris.persistence.cache.CaffeineCacheBackend$CacheKeyValue object internals:
   * OFF  SZ               TYPE DESCRIPTION                         VALUE
   *   0   8                    (object header: mark)               N/A
   *   8   8                    (object header: class)              N/A
   *  16   8               long CacheKeyValue.expiresAtNanosEpoch   N/A
   *  24   4                int CacheKeyValue.hash                  N/A
   *  28   4                    (alignment/padding gap)
   *  32   8   java.lang.String CacheKeyValue.realmId               N/A
   *  40   8             byte[] CacheKeyValue.key                   N/A
   *  48   8             byte[] CacheKeyValue.serialized            N/A
   * Instance size: 56 bytes
   * Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
   * </pre></code>
   */
  static final int CACHE_KEY_VALUE_SIZE = 64;

  /**
   * Worst layout of {@code java.lang.String}: <code><pre>
   * ***** Hotspot Layout Simulation (JDK 15, 64-bit model, NO compressed references, NO compressed classes, 8-byte aligned)
   *
   * java.lang.String object internals:
   * OFF  SZ      TYPE DESCRIPTION               VALUE
   *   0   8           (object header: mark)     N/A
   *   8   8           (object header: class)    N/A
   *  16   4       int String.hash               N/A
   *  20   1      byte String.coder              N/A
   *  21   1   boolean String.hashIsZero         N/A
   *  22   2           (alignment/padding gap)
   *  24   8    byte[] String.value              N/A
   * Instance size: 32 bytes
   * Space losses: 2 bytes internal + 0 bytes external = 2 bytes total
   * </pre></code>
   */
  static final int STRING_SIZE = 32;

  static final int ARRAY_OVERHEAD = 16;
  static final int CAFFEINE_OBJ_OVERHEAD = 2 * 32;

  static final ObjectMapper SMILE_MAPPER =
      new SmileMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectWriter OBJ_WRITER = SMILE_MAPPER.writer().withView(Object.class);

  static byte[] serializeObj(Obj obj) {
    try {
      return OBJ_WRITER.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] serializeReference(Reference ref) {
    try {
      return SMILE_MAPPER.writeValueAsBytes(ref);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
