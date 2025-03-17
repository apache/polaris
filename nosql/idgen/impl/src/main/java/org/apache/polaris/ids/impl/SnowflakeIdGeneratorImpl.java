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
package org.apache.polaris.ids.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;

/**
 * Implementation of a local, per-node generator for so-called "snowflake IDs", which are unique
 * integer IDs in a distributed environment.
 *
 * <p>A monotonically increasing clock is <em>strictly required</em>. Invocations of {@link
 * #generateId()} fail hard, if the clock walks backwards, which means it returns a lower value than
 * before. It is recommended to use an implementation of {@link MonotonicClock} as the clock source.
 *
 * <p>The implementation is thread-safe.
 *
 * <p>Reference: <a
 * href="https://medium.com/@jitenderkmr/demystifying-snowflake-ids-a-unique-identifier-in-distributed-computing-72796a827c9d">Article
 * on medium.com</a>, <a
 * href="https://github.com/twitter-archive/snowflake/tree/b3f6a3c6ca8e1b6847baa6ff42bf72201e2c2231">Twitter
 * GitHub repository (archived)</a>
 */
class SnowflakeIdGeneratorImpl implements SnowflakeIdGenerator {

  // TODO add a specialized implementation using hard-coded values for the standardized parameters

  private static final AtomicLongFieldUpdater<SnowflakeIdGeneratorImpl> LAST_ID_UPDATER =
      AtomicLongFieldUpdater.newUpdater(SnowflakeIdGeneratorImpl.class, "lastId");
  private final BooleanSupplier validationCallback;

  // Used in hot generateId()
  private volatile long lastId;
  private final LongSupplier clockMillis;
  private final long epochOffset;
  private final long timestampMax;
  private final int timestampShift;
  private final int sequenceBits;
  private final long sequenceMask;
  private final long nodeIdPattern;
  private final long nodeMask;

  SnowflakeIdGeneratorImpl(
      int nodeId, @Nonnull LongSupplier clockMillis, BooleanSupplier validationCallback) {
    this(
        DEFAULT_TIMESTAMP_BITS,
        DEFAULT_SEQUENCE_BITS,
        DEFAULT_NODE_ID_BITS,
        nodeId,
        EPOCH_OFFSET_MILLIS,
        requireNonNull(clockMillis, "Clock must not be null"),
        validationCallback);
  }

  SnowflakeIdGeneratorImpl(
      int timestampBits,
      int sequenceBits,
      int nodeBits,
      int nodeId,
      long epochOffset,
      @Nonnull LongSupplier clockMillis,
      @Nonnull BooleanSupplier validationCallback) {
    validateArguments(timestampBits, sequenceBits, nodeBits, nodeId, epochOffset, clockMillis);
    this.timestampShift = sequenceBits + nodeBits;
    this.timestampMax = 1L << timestampBits;
    this.nodeMask = (1L << nodeBits) - 1;
    this.sequenceBits = sequenceBits;
    this.sequenceMask = (1L << sequenceBits) - 1;
    this.epochOffset = epochOffset;
    this.clockMillis = clockMillis;
    this.nodeIdPattern = ((long) nodeId) << sequenceBits;
    this.validationCallback = validationCallback;
  }

  static void validateArguments(
      int timestampBits,
      int sequenceBits,
      int nodeBits,
      int nodeId,
      long epochOffset,
      LongSupplier clockMillis) {
    checkNotNull(clockMillis, "Clock must not be null");
    var nowMillis = clockMillis.getAsLong();
    var now = Instant.ofEpochMilli(nowMillis);
    var nodeMax = 1L << nodeBits;
    var timestampMax = 1L << timestampBits;
    checkArgs(
        () ->
            checkArgument(
                nowMillis >= epochOffset,
                "Clock returns a timestamp %s less than the configured epoch %s",
                now,
                Instant.ofEpochMilli(epochOffset)),
        () ->
            checkArgument(
                nowMillis - epochOffset < timestampMax,
                "Clock already returns a timestamp %s greater of after %s",
                now,
                Instant.ofEpochMilli(timestampMax)),
        () ->
            checkArgument(
                nodeBits >= 2
                    && sequenceBits >= 5
                    && timestampBits >= 5 // this is REALLY low !
                    && nodeBits < 64
                    && sequenceBits < 64
                    && timestampBits < 64,
                "value of nodeBits %s or sequenceBits %s or timestampBits %s is too low or too high",
                nodeBits,
                sequenceBits,
                timestampBits),
        () ->
            checkArgument(
                timestampBits + nodeBits + sequenceBits == 63,
                "Sum of timestampBits + nodeBits + sequenceBits must be == 63"),
        () ->
            checkArgument(
                nodeId >= 0 && nodeId < nodeMax,
                "nodeId %s out of range [0..%s[",
                nodeId,
                nodeMax));
  }

  static void checkArgs(Runnable... checks) {
    var violations = new ArrayList<String>();
    for (Runnable check : checks) {
      try {
        check.run();
      } catch (IllegalArgumentException iae) {
        violations.add(iae.getMessage());
      }
    }
    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(String.join(", ", violations));
    }
  }

  @Override
  public long systemIdForNode(int nodeId) {
    return constructIdUnsafe(timestampMax - 1, 0, nodeId);
  }

  private long constructIdUnsafe(long timestamp, long sequence, long nodeId) {
    return (timestamp << timestampShift) | (nodeId << sequenceBits) | sequence;
  }

  @Override
  public long constructId(long timestamp, long sequence, long nodeId) {
    checkArgument(
        (timestamp & (timestampMax - 1)) != timestampMax - 1,
        "timestamp argument %s out of range",
        timestamp);
    checkArgument(
        (sequence & sequenceMask) == sequence, "sequence argument %s out of range", sequence);
    checkArgument((nodeId & nodeMask) == nodeId, "nodeId argument %s out of range", nodeId);
    return constructIdUnsafe(timestamp, sequence, nodeId);
  }

  @Override
  public long generateId() {
    checkState(validationCallback.getAsBoolean(), "Cannot generate a new ID, shutting down?");

    var needTimestamp = true;
    var timestamp = 0L;

    while (true) {
      var last = LAST_ID_UPDATER.get(this);
      var lastTimestamp = timestampFromId(last);

      if (needTimestamp || timestamp < lastTimestamp) {
        // MUST query the clock AFTER fetching 'lastId', otherwise a concurrent thread might update
        // 'lastId' with a newer clock value and the monotonic-clock-source check would fail.
        timestamp = clockMillis.getAsLong() - epochOffset;
        checkState(
            timestamp < timestampMax,
            "Cannot generate any more IDs as the lifetime of the generator has expired");
        if (timestamp < lastTimestamp) {
          throw new IllegalStateException(
              "Clock walked backwards from "
                  + lastTimestamp
                  + " to "
                  + timestamp
                  + ", provide a monotonically increasing clock source");
        }
        needTimestamp = false;
      }

      long sequence;
      if (lastTimestamp == timestamp) {
        sequence = sequenceFromId(last);
        if (sequence == sequenceMask) {
          // last generated sequence for the current millisecond yielded the maximum value,
          // spin-wait until the next millisecond
          spinWaitSequence();
          // Force re-fetching the timestamp
          needTimestamp = true;
          continue;
        }
        sequence++;
      } else {
        sequence = 0L;
      }

      holdForTest();

      var id = (timestamp << timestampShift) | nodeIdPattern | sequence;

      if (LAST_ID_UPDATER.compareAndSet(this, last, id)) {
        return id;
      }

      spinWaitRace();
      // Do not re-fetch the timestamp from the clock source (a bit faster)
    }
  }

  @VisibleForTesting
  void holdForTest() {}

  @VisibleForTesting
  void spinWaitSequence() {
    try {
      // Sleep for 0.5ms - no Thread.yield() or Thread.onSpinWait(), because those cause too much
      // CPU load
      Thread.sleep(0, 500_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  void spinWaitRace() {
    Thread.onSpinWait();
  }

  @Override
  public long timestampFromId(long id) {
    return id >>> timestampShift;
  }

  @Override
  public long timestampUtcFromId(long id) {
    return timestampFromId(id) + epochOffset;
  }

  @Override
  public long sequenceFromId(long id) {
    return id & sequenceMask;
  }

  @Override
  public long nodeFromId(long id) {
    return (id >>> sequenceBits) & nodeMask;
  }

  @Override
  public UUID idToTimeUuid(long id) {
    var timestamp = timestampFromId(id);
    var sequence = sequenceFromId(id);
    var node = nodeFromId(id);

    return new UUID(timeUuidMsb(timestamp), timeUuidLsb(sequence, node));
  }

  @Override
  public long timeUuidToId(@Nonnull UUID uuid) {
    checkArgument(
        uuid.variant() == 2 && uuid.version() == 1, "Must be a version 1 / variant 2 UUID");
    var ts = uuid.timestamp() - epochOffset;
    var seq = uuid.clockSequence();
    var node = uuid.node();
    checkArgument(
        ts > 0
            && ts <= timestampMax
            && seq >= 0
            && seq <= sequenceMask
            && node >= 0
            && node <= nodeMask,
        "TimeUUID contains values that cannot be condensed into a snowflake-ID");
    return constructId(ts, seq, node);
  }

  @Override
  public String describeId(long id) {
    var ts = timestampFromId(id);
    var seq = sequenceFromId(id);
    var node = nodeFromId(id);
    var tsUnixEpoch = ts + epochOffset;
    var instant = Instant.ofEpochMilli(tsUnixEpoch);
    var zone = ZoneOffset.systemDefault();
    var local = LocalDateTime.ofInstant(instant, zone);
    return format(
        """
        Snowflake-ID %d components
                         timestamp : %d
                              node : %d%s
                          sequence : %d
                    timestamp/Unix : %d (= timestamp + epoch offset)
                 timestamp/instant : %s
                   timestamp/local : %s %s
                  generator offset : %d / %s
        """,
        id,
        ts,
        node,
        (ts == 0L && seq == 0L) ? " (system ID for this node)" : "",
        seq,
        tsUnixEpoch,
        instant,
        local,
        zone,
        epochOffset,
        Instant.ofEpochMilli(epochOffset));
  }

  @Override
  public int timestampBits() {
    return Long.numberOfTrailingZeros(timestampMax);
  }

  @Override
  public int sequenceBits() {
    return sequenceBits;
  }

  @Override
  public int nodeIdBits() {
    return 64 - Long.numberOfLeadingZeros(nodeMask);
  }

  @Override
  public String idToString(long id) {
    var ts = timestampFromId(id);
    return Instant.ofEpochMilli(ts + epochOffset).toString()
        + " ("
        + ts
        + "), sequence "
        + sequenceFromId(id)
        + ", node "
        + nodeFromId(id);
  }

  @VisibleForTesting
  static long timeUuidLsb(long sequence, long node) {
    // LSB:
    //  0xC000000000000000 variant
    //  0x3FFF000000000000 clock_seq
    //  0x0000FFFFFFFFFFFF node

    return
    // variant
    0x8000000000000000L
        // clock_seq
        | ((sequence << 48) & 0x3FFF000000000000L)
        // node
        | (node & 0x0000FFFFFFFFFFFFL);
  }

  @VisibleForTesting
  private long timeUuidMsb(long timestamp) {
    return timeUuidMsbReal(timestamp + epochOffset);
  }

  @VisibleForTesting
  static long timeUuidMsbReal(long timestamp) {
    // MSB:
    //  0xFFFFFFFF00000000 time_low
    //  0x00000000FFFF0000 time_mid
    //  0x000000000000F000 version
    //  0x0000000000000FFF time_hi

    return
    // time_low
    (timestamp << 32 & 0xFFFFFFFF00000000L)
        |
        // time_mid
        ((timestamp >>> (32 - 16) & 0x00000000FFFF0000L))
        |
        // version
        0x0000000000001000L
        |
        // time_hi
        ((timestamp >>> 48) & 0x0000000000000FFFL);
  }
}
