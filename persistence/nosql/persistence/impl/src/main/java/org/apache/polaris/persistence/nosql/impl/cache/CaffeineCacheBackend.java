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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.contextualReader;
import static org.apache.polaris.persistence.nosql.api.obj.ObjType.CACHE_UNLIMITED;
import static org.apache.polaris.persistence.nosql.api.obj.ObjType.NOT_CACHED;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static tools.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

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
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.smile.SmileMapper;

class CaffeineCacheBackend implements CacheBackend {

  private static final byte KIND_REFERENCE = 0;
  private static final byte KIND_FLAG_OBJ_ID = 1;
  private static final byte KIND_FLAG_NUM_PARTS = 2;
  private static final byte KIND_FLAG_CREATED = 4;
  private static final byte KIND_FLAG_VERSION = 8;

  private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineCacheBackend.class);

  public static final String METER_CACHE_CAPACITY = "cache.capacity";
  public static final String METER_CACHE_ADMIT_CAPACITY = "cache.capacity.admitted";
  public static final String METER_CACHE_WEIGHT = "cache.weight-reported";
  public static final String METER_CACHE_REJECTED_WEIGHT = "cache.rejected-weight";

  public static final String CACHE_NAME = "polaris-objects";

  private final CacheConfig config;
  final Cache<CacheKey, CacheValue> cache;

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
                new Expiry<CacheKey, CacheValue>() {
                  @Override
                  public long expireAfterCreate(
                      @NonNull CacheKey key, @NonNull CacheValue value, long currentTimeNanos) {
                    var expire = value.expiresAtNanosEpoch;
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
                      @NonNull CacheKey key,
                      @NonNull CacheValue value,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return expireAfterCreate(key, value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterRead(
                      @NonNull CacheKey key,
                      @NonNull CacheValue value,
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
                          .description("Weight of rejected cache-puts in bytes.")
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
  public Persistence wrap(@NonNull Persistence persist) {
    return new CachingPersistenceImpl(persist, this);
  }

  private int weigher(CacheKey key, CacheValue value) {
    var size = key.heapSize() + value.heapSize();
    size += CAFFEINE_OBJ_OVERHEAD;
    return size;
  }

  @Override
  public Obj get(@NonNull String realmId, @NonNull ObjRef id) {
    var key = cacheKeyObjRead(realmId, id);
    var value = cache.getIfPresent(key);
    if (value == null) {
      return null;
    }
    if (value.serialized == null) {
      return NOT_FOUND_OBJ_SENTINEL;
    }
    return value.getObj();
  }

  @Override
  public void put(@NonNull String realmId, @NonNull Obj obj) {
    putLocal(realmId, obj);
  }

  @VisibleForTesting
  void cachePut(CacheKey key, CacheValue value) {
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
  public void putLocal(@NonNull String realmId, @NonNull Obj obj) {
    long expiresAt =
        obj.type()
            .cachedObjectExpiresAtMicros(
                obj, () -> NANOSECONDS.toMicros(config.clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      return;
    }

    var expiresAtNanos =
        expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
    cachePut(cacheKeyObjRead(realmId, objRef(obj)), cacheValueObj(obj, expiresAtNanos));
  }

  @Override
  public void putNegative(@NonNull String realmId, @NonNull ObjRef id) {
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
    cachePut(cacheKeyObjRead(realmId, id), new CacheValue(null, expiresAtNanos, null));
  }

  @Override
  public void remove(@NonNull String realmId, @NonNull ObjRef id) {
    cache.invalidate(cacheKeyObjRead(realmId, id));
  }

  @Override
  public void clear(@NonNull String realmId) {
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
  public void removeReference(@NonNull String realmId, @NonNull String name) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    cache.invalidate(cacheKeyReferenceRead(realmId, name));
  }

  @Override
  public void putReference(@NonNull String realmId, @NonNull Reference reference) {
    putReferenceLocal(realmId, reference);
  }

  @Override
  public void putReferenceLocal(@NonNull String realmId, @NonNull Reference reference) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    var expiresAtNanos = config.clockNanos().getAsLong() + refCacheTtlNanos;
    cachePut(
        cacheKeyReferenceRead(realmId, reference.name()),
        new CacheValue(null, expiresAtNanos, serializeReference(reference)));
  }

  @Override
  public void putReferenceNegative(@NonNull String realmId, @NonNull String name) {
    if (refCacheNegativeTtlNanos <= 0L) {
      return;
    }
    cachePut(
        cacheKeyReferenceRead(realmId, name),
        new CacheValue(null, config.clockNanos().getAsLong() + refCacheNegativeTtlNanos, null));
  }

  @Override
  public Reference getReference(@NonNull String realmId, @NonNull String name) {
    if (refCacheTtlNanos <= 0L) {
      return null;
    }
    var value = cache.getIfPresent(cacheKeyReferenceRead(realmId, name));
    if (value == null) {
      return null;
    }
    if (value.serialized == null) {
      return NON_EXISTENT_REFERENCE_SENTINEL;
    }
    return value.getReference();
  }

  private static CacheValue cacheValueObj(@NonNull Obj obj, long expiresAtNanos) {
    return new CacheValue(cacheKeyObj(obj), expiresAtNanos, serializeObj(obj));
  }

  @VisibleForTesting
  static CacheKey cacheKeyObjRead(@NonNull String realmId, @NonNull ObjRef id) {
    return new CacheKey(realmId, cacheKeyObjId(id));
  }

  @VisibleForTesting
  static CacheKey cacheKeyReferenceRead(@NonNull String realmId, @NonNull String name) {
    return new CacheKey(realmId, cacheKeyReference(name));
  }

  private static byte[] cacheKeyObj(@NonNull Obj obj) {
    return cacheKeyObjId(
        obj.type().id(), obj.id(), obj.numParts(), obj.createdAtMicros(), obj.versionToken());
  }

  private static byte[] cacheKeyObjId(@NonNull ObjRef id) {
    return cacheKeyObjId(id.type(), id.id(), 1, 0L, null);
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

  private static byte[] cacheKeyReference(String refName) {
    var refNameBytes = refName.getBytes(UTF_8);
    var key = new byte[1 + refNameBytes.length];
    var buf = ByteBuffer.wrap(key);
    buf.put(KIND_REFERENCE);
    buf.put(refNameBytes);
    return key;
  }

  /** Identity only: Caffeine can retain this key when replacing an equal entry's value. */
  static final class CacheKey {

    final String realmId;
    final byte[] key;
    final int hash;

    CacheKey(String realmId, byte[] key) {
      this.realmId = realmId;
      this.key = key;

      this.hash = 31 * realmId.hashCode() + Arrays.hashCode(key);
    }

    /** Conservative estimate of this key's heap usage, including its identity bytes. */
    int heapSize() {
      return CACHE_KEY_SIZE
          + STRING_SIZE
          + ARRAY_OVERHEAD
          + realmId.length()
          + ARRAY_OVERHEAD
          + key.length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey that)) {
        return false;
      }
      return realmId.equals(that.realmId) && Arrays.equals(key, that.key);
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
      }

      return sb.append("}").toString();
    }
  }

  /** All replaceable state belongs to the value, including object deserialization metadata. */
  record CacheValue(byte[] objectMetadata, long expiresAtNanosEpoch, byte[] serialized) {
    int heapSize() {
      return CACHE_VALUE_SIZE
          + (objectMetadata != null ? ARRAY_OVERHEAD + objectMetadata.length : 0)
          + (serialized != null ? ARRAY_OVERHEAD + serialized.length : 0);
    }

    Obj getObj() {
      var buf = ByteBuffer.wrap(objectMetadata);
      var kind = buf.get();
      checkState(kind != KIND_REFERENCE, "Cache value content is not an object");
      var relevantLen = readVarInt(buf);
      var id = buf.getLong();
      var typeIdLen = relevantLen - Long.BYTES;
      var typeId = new String(objectMetadata, buf.position(), typeIdLen, UTF_8);
      var type = objTypeById(typeId);
      buf.position(buf.position() + typeIdLen);
      var numParts = ((kind & KIND_FLAG_NUM_PARTS) == KIND_FLAG_NUM_PARTS) ? readVarInt(buf) : 1;
      var createdAtMicros = ((kind & KIND_FLAG_CREATED) == KIND_FLAG_CREATED) ? buf.getLong() : 0L;
      var versionToken =
          ((kind & KIND_FLAG_VERSION) == KIND_FLAG_VERSION)
              ? new String(
                  objectMetadata, buf.position(), objectMetadata.length - buf.position(), UTF_8)
              : null;

      return contextualReader(SMILE_MAPPER, type, id, numParts, versionToken, createdAtMicros)
          .readValue(serialized);
    }

    Reference getReference() {
      return SMILE_MAPPER.readValue(serialized, Reference.class);
    }
  }

  // Conservative instance sizes with uncompressed references and 8-byte alignment:
  // key: 16-byte header, two references, int hash, padding;
  // value: 16-byte header, two references, long expiry.
  static final int CACHE_KEY_SIZE = 40;
  static final int CACHE_VALUE_SIZE = 40;

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
      SmileMapper.builder()
          .findAndAddModules()
          .disable(FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DEFAULT_VIEW_INCLUSION)
          .build();
  private static final ObjectWriter OBJ_WRITER = SMILE_MAPPER.writer().withView(Object.class);

  static byte[] serializeObj(Obj obj) {
    return OBJ_WRITER.writeValueAsBytes(obj);
  }

  static byte[] serializeReference(Reference ref) {
    return SMILE_MAPPER.writeValueAsBytes(ref);
  }
}
