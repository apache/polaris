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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_TIMESTAMP_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.ID_EPOCH_MILLIS;
import static org.apache.polaris.ids.impl.SnowflakeIdGeneratorImpl.timeUuidLsb;
import static org.apache.polaris.ids.impl.SnowflakeIdGeneratorImpl.timeUuidMsbReal;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@Timeout(value = 5, unit = MINUTES)
public class TestSnowflakeIdGeneratorImpl {
  public static final IdGeneratorSource ID_GENERATOR_SOURCE_CONSTANT =
      new IdGeneratorSource() {
        @Override
        public long currentTimeMillis() {
          return ID_EPOCH_MILLIS;
        }

        @Override
        public int nodeId() {
          return 0;
        }
      };
  @InjectSoftAssertions protected SoftAssertions soft;

  protected MonotonicClock clock;

  @SuppressWarnings("resource")
  @BeforeEach
  protected void setUp() {
    clock = MonotonicClockImpl.newDefaultInstance();
  }

  @AfterEach
  protected void tearDown() {
    clock.close();
  }

  @Test
  public void validArgs() {
    soft.assertThatCode(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(Map.of(), idGeneratorSource(0, clock::currentTimeMillis)))
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        Map.of(),
                        idGeneratorSource(
                            (1 << DEFAULT_NODE_ID_BITS) - 1, clock::currentTimeMillis)))
        .doesNotThrowAnyException();
  }

  @Test
  public void invalidArgs() {
    var validClock = (LongSupplier) () -> ID_EPOCH_MILLIS;

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(Map.of(), idGeneratorSource(-1, () -> 0L)))
        .withMessage(
            "Clock returns a timestamp 1970-01-01T00:00:00Z less than the configured epoch 2025-03-01T00:00:00Z, nodeId -1 out of range [0..1024[");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(Map.of(), idGeneratorSource(-1, validClock)))
        .withMessage("nodeId -1 out of range [0..1024[");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        Map.of(), idGeneratorSource(1 << DEFAULT_NODE_ID_BITS, validClock)))
        .withMessage("nodeId 1024 out of range [0..1024[");

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        31,
                        DEFAULT_SEQUENCE_BITS,
                        DEFAULT_NODE_ID_BITS,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage("Sum of timestampBits + nodeBits + sequenceBits must be == 63");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        4,
                        DEFAULT_NODE_ID_BITS,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage(
            "value of nodeBits 10 or sequenceBits 4 or timestampBits 41 is too low or too high, Sum of timestampBits + nodeBits + sequenceBits must be == 63");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        DEFAULT_SEQUENCE_BITS,
                        4,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage("Sum of timestampBits + nodeBits + sequenceBits must be == 63");

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        64,
                        DEFAULT_SEQUENCE_BITS,
                        DEFAULT_NODE_ID_BITS,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage(
            "value of nodeBits 10 or sequenceBits 12 or timestampBits 64 is too low or too high, Sum of timestampBits + nodeBits + sequenceBits must be == 63");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        64,
                        DEFAULT_NODE_ID_BITS,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage(
            "value of nodeBits 10 or sequenceBits 64 or timestampBits 41 is too low or too high, Sum of timestampBits + nodeBits + sequenceBits must be == 63");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        DEFAULT_SEQUENCE_BITS,
                        64,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage(
            "value of nodeBits 64 or sequenceBits 12 or timestampBits 41 is too low or too high, Sum of timestampBits + nodeBits + sequenceBits must be == 63");

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        5,
                        DEFAULT_NODE_ID_BITS,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage("Sum of timestampBits + nodeBits + sequenceBits must be == 63");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnowflakeIdGeneratorFactory()
                    .buildIdGenerator(
                        DEFAULT_TIMESTAMP_BITS,
                        4,
                        18,
                        ID_EPOCH_MILLIS,
                        ID_GENERATOR_SOURCE_CONSTANT))
        .withMessage(
            "value of nodeBits 18 or sequenceBits 4 or timestampBits 41 is too low or too high");
  }

  @Test
  public void clockBackwards() {
    var clock = new AtomicLong(ID_EPOCH_MILLIS + 100);

    var nodeId = 42;

    var ids = new HashSet<Long>();

    var impl =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(Map.of(), idGeneratorSource(nodeId, clock::get));

    soft.assertThatCode(() -> soft.assertThat(ids.add(impl.generateId())).isTrue())
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> soft.assertThat(ids.add(impl.generateId())).isTrue())
        .doesNotThrowAnyException();
    clock.addAndGet(20);
    soft.assertThatCode(() -> soft.assertThat(ids.add(impl.generateId())).isTrue())
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> soft.assertThat(ids.add(impl.generateId())).isTrue())
        .doesNotThrowAnyException();
    clock.addAndGet(-1);
    soft.assertThatIllegalStateException()
        .isThrownBy(impl::generateId)
        .withMessage(
            "Clock walked backwards from 120 to 119, provide a monotonically increasing clock source");
    clock.addAndGet(1);
    soft.assertThatCode(() -> soft.assertThat(ids.add(impl.generateId())).isTrue())
        .doesNotThrowAnyException();
  }

  /**
   * Tests concurrency in {@link SnowflakeIdGeneratorImpl}, forcing CAS update races on the {@code
   * lastId} field and asserting that {@link SnowflakeIdGeneratorImpl#spinWaitRace()} has been
   * called.
   */
  @Test
  @Timeout(value = 5, unit = MINUTES)
  public void concurrency() throws Exception {
    var clock = new AtomicLong(ID_EPOCH_MILLIS + 100);

    var nodeId = 42;

    var holdEnterT1 = new Semaphore(0);
    var holdEnterT2 = new Semaphore(0);
    var holdWaitT1 = new Semaphore(0);
    var holdWaitT2 = new Semaphore(0);
    var spinEnterT2 = new Semaphore(0);
    var spinWaitT2 = new Semaphore(0);

    var syncGenerate = new CountDownLatch(1);
    var syncMain = new CountDownLatch(2);
    var thread1 = new AtomicReference<Thread>();
    var impl =
        new SnowflakeIdGeneratorImpl(idGeneratorSource(nodeId, clock::get)) {
          @Override
          void holdForTest() {
            var isT1 = thread1.get() == Thread.currentThread();

            (isT1 ? holdEnterT1 : holdEnterT2).release();
            (isT1 ? holdWaitT1 : holdWaitT2).acquireUninterruptibly();
          }

          @Override
          void spinWaitRace() {
            var isT1 = thread1.get() == Thread.currentThread();

            checkState(!isT1);

            spinEnterT2.release();
            spinWaitT2.acquireUninterruptibly();
          }
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {

      var t1 =
          executorService.submit(
              () -> {
                thread1.set(Thread.currentThread());

                syncMain.countDown();
                syncGenerate.await();

                return impl.generateId();
              });

      var t2 =
          executorService.submit(
              () -> {
                syncMain.countDown();
                syncGenerate.await();

                return impl.generateId();
              });

      // Wait for both threads to reach the same state
      syncMain.await();
      // Let both threads continue
      syncGenerate.countDown();

      // Wait until both threads reached the "holdForTest()" function after loading the 'lastId'
      // field with the same value
      holdEnterT1.acquireUninterruptibly();
      holdEnterT2.acquireUninterruptibly();

      // Let T1 continue (and finish) - updates 'lastId' field
      holdWaitT1.release();
      // Get T1's result
      var id1 = t1.get();

      // Let T2 continue - it will race updating the 'lastId' field
      holdWaitT2.release();

      // Wait until T2 enters "spinWait()"
      spinEnterT2.acquireUninterruptibly();
      // Let T2 retry
      spinWaitT2.release();

      // Wait until T2 reached "holdForTest()" after hitting the 'lastId' update race
      holdEnterT2.acquireUninterruptibly();
      // Let T2 continue - next iteration should not race
      holdWaitT2.release();

      var id2 = t2.get();

      soft.assertThat(id1).isNotEqualTo(id2);
    }
  }

  @Test
  public void manyThreads() throws Exception {
    var threads = Runtime.getRuntime().availableProcessors() * 2;
    var nodeId = 42;
    var impl =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(Map.of(), idGeneratorSource(nodeId, clock::currentTimeMillis));

    var sync = new CountDownLatch(threads);
    var start = new CountDownLatch(1);
    var done = new CountDownLatch(threads);
    var finish = new CountDownLatch(1);
    var numIdsPerThread = 5000;

    try (var executorService = Executors.newFixedThreadPool(threads)) {
      var ids = ConcurrentHashMap.newKeySet(numIdsPerThread * threads * 2);
      var futures = new ArrayList<Future<?>>(threads);

      for (int i = 0; i < threads; i++) {
        futures.add(
            executorService.submit(
                () -> {
                  sync.countDown();
                  start.await();

                  var localIds = new HashSet<Long>(numIdsPerThread * 2);
                  try {
                    for (int n = 0; n < numIdsPerThread; n++) {
                      localIds.add(impl.generateId());
                    }
                  } finally {
                    done.countDown();
                    finish.await();
                  }

                  ids.addAll(localIds);

                  return null;
                }));
      }

      // Wait until all threads have started
      sync.await();
      // Let threads start
      start.countDown();

      // Wait until all threads have started
      done.await();
      // Let threads start
      finish.countDown();

      for (Future<?> future : futures) {
        future.get();
      }

      soft.assertThat(ids).hasSize(numIdsPerThread * threads);
    }
  }

  @Test
  public void maxIdsPerMillisecondAtEpochOffset() {
    var clock = (LongSupplier) () -> ID_EPOCH_MILLIS;

    var nodeId = 42;

    var impl =
        new SnowflakeIdGeneratorImpl(idGeneratorSource(nodeId, clock)) {
          @Override
          void spinWaitSequence() {
            throw new RuntimeException("Moo");
          }

          @Override
          void spinWaitRace() {
            throw new RuntimeException("Moo");
          }
        };

    // Implementation detail: the 1st sequence if the timestamp is equal to EPOCH_OFFSET is 1.
    // All other initial timestamps start with sequence == 0.
    var maxPerMillisecond = 4095;

    var ids = new long[maxPerMillisecond];
    for (var i = 0; i < maxPerMillisecond; i++) {
      ids[i] = impl.generateId();
    }

    soft.assertThatThrownBy(impl::generateId)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Moo");
    soft.assertThatThrownBy(impl::generateId)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Moo");

    var expect = ((long) nodeId) << DEFAULT_SEQUENCE_BITS | 1L;
    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      soft.assertThat(id).describedAs("index %d", i).isEqualTo(expect);
      soft.assertThat(impl.nodeFromId(id)).isEqualTo(nodeId);
      soft.assertThat(impl.timestampFromId(id)).isEqualTo(0L);
      soft.assertThat(impl.sequenceFromId(id)).isEqualTo(i + 1);
      soft.assertThat(impl.idToString(id))
          .isEqualTo(
              format(
                  "%s (%d), sequence %d, node %d",
                  Instant.ofEpochMilli(ID_EPOCH_MILLIS), 0, i + 1, nodeId));
      expect++;
      if (i % 10 == 0) {
        soft.assertAll();
      }
    }
  }

  @Test
  public void maxIdsPerMillisecondAtNowWithMutableClock() {
    var clockSource = new AtomicLong(ID_EPOCH_MILLIS + TimeUnit.DAYS.toMillis(365));
    var clock = (LongSupplier) clockSource::get;
    var initialTimestamp = clock.getAsLong() - ID_EPOCH_MILLIS;

    var nodeId = 42;

    var impl =
        new SnowflakeIdGeneratorImpl(idGeneratorSource(nodeId, clock)) {
          @Override
          void spinWaitSequence() {
            clockSource.incrementAndGet();
          }
        };

    for (var millis = 0; millis < 5; millis++) {
      var expect =
          ((initialTimestamp + millis) << (DEFAULT_SEQUENCE_BITS + DEFAULT_NODE_ID_BITS))
              |
              //
              ((long) nodeId) << DEFAULT_SEQUENCE_BITS;

      for (var j = 0; j < 4096; j++) {
        var id = impl.generateId();
        var uuid = impl.idToTimeUuid(id);

        soft.assertThat(impl.nodeFromId(id)).isEqualTo(nodeId).isEqualTo(uuid.node());
        soft.assertThat(impl.timestampFromId(id))
            .isEqualTo(initialTimestamp + millis)
            .isEqualTo(uuid.timestamp() - ID_EPOCH_MILLIS);
        soft.assertThat(uuid).extracting(UUID::variant, UUID::version).containsExactly(2, 1);
        soft.assertThat(impl.timeUuidToId(uuid)).isEqualTo(id);
        soft.assertThat(impl.timestampUtcFromId(id)).isEqualTo(uuid.timestamp());
        soft.assertThat(impl.sequenceFromId(id)).isEqualTo(j).isEqualTo(uuid.clockSequence());
        soft.assertThat(impl.idToString(id))
            .isEqualTo(
                format(
                    "%s (%d), sequence %d, node %d",
                    Instant.ofEpochMilli(ID_EPOCH_MILLIS + initialTimestamp + millis),
                    initialTimestamp + millis,
                    j,
                    nodeId));
        soft.assertThat(id).describedAs("millis %d - seq %d", millis, j).isEqualTo(expect);
        expect++;
        if (j % 10 == 0) {
          soft.assertAll();
        }
      }
    }
  }

  @Test
  public void miscUuid() {
    var clockSource = new AtomicLong(ID_EPOCH_MILLIS + TimeUnit.DAYS.toMillis(365));
    var clock = (LongSupplier) clockSource::get;

    var nodeId = 42;

    var impl =
        new SnowflakeIdGeneratorImpl(idGeneratorSource(nodeId, clock)) {
          @Override
          void spinWaitSequence() {
            clockSource.incrementAndGet();
          }
        };

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> impl.timeUuidToId(UUID.randomUUID()))
        .withMessage("Must be a version 1 / variant 2 UUID");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> impl.timeUuidToId(UUID.nameUUIDFromBytes("foobar".getBytes(UTF_8))))
        .withMessage("Must be a version 1 / variant 2 UUID");

    long tsUuidHighest = 0xFFFFFFFFFFFFFFFL;
    long seqUuidHighest = 0xFFFFFFFFFFFFL;
    long nodeUuidHighest = 0x3FFF;

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                impl.timeUuidToId(
                    new UUID(
                        timeUuidMsbReal(tsUuidHighest),
                        timeUuidLsb(seqUuidHighest, nodeUuidHighest))))
        .withMessage("TimeUUID contains values that cannot be condensed into a snowflake-ID");

    soft.assertThatCode(
            () ->
                impl.timeUuidToId(
                    new UUID(
                        timeUuidMsbReal((1L << DEFAULT_TIMESTAMP_BITS) - 1),
                        timeUuidLsb(
                            (1L << DEFAULT_SEQUENCE_BITS) - 1, (1L << DEFAULT_NODE_ID_BITS) - 1))))
        .doesNotThrowAnyException();
  }

  @Test
  public void maxIdsPerMillisecondAtNowWithRealClock() {
    var nodeId = 42;

    var initialTimestamp = clock.currentTimeMillis() - ID_EPOCH_MILLIS;
    var impl =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(Map.of(), idGeneratorSource(nodeId, clock::currentTimeMillis));

    var ids = new HashSet<Long>();
    for (var i = 0; i < 10 * 4096; i++) {

      var id = impl.generateId();
      soft.assertThat(ids.add(id)).isTrue();

      var uuid = impl.idToTimeUuid(id);

      soft.assertThat(impl.nodeFromId(id)).isEqualTo(nodeId).isEqualTo(uuid.node());
      soft.assertThatCode(
              () -> {
                assertThat(uuid.node()).isEqualTo(nodeId);
                assertThat(uuid.timestamp())
                    .isGreaterThan(ID_EPOCH_MILLIS)
                    .isEqualTo(impl.timestampUtcFromId(id));
                assertThat(uuid.clockSequence()).isGreaterThanOrEqualTo(0).isLessThan(4096);
                assertThat(uuid.variant()).isEqualTo(2);
                assertThat(uuid.version()).isEqualTo(1);
              })
          .doesNotThrowAnyException();
      soft.assertThat(impl.timeUuidToId(uuid)).isEqualTo(id);
      soft.assertThat(impl.timestampFromId(id)).isGreaterThanOrEqualTo(initialTimestamp);
      soft.assertThat(impl.sequenceFromId(id))
          .isGreaterThanOrEqualTo(0)
          .isLessThan(4096)
          .isEqualTo(uuid.clockSequence());
      soft.assertThat(impl.idToString(id))
          .isEqualTo(
              format(
                  "%s (%d), sequence %d, node %d",
                  Instant.ofEpochMilli(impl.timestampFromId(id) + ID_EPOCH_MILLIS),
                  impl.timestampFromId(id),
                  impl.sequenceFromId(id),
                  nodeId));

      if (i % 10 == 0) {
        soft.assertAll();
      }
    }
  }

  @Test
  public void validationCallback() {
    var nodeId = 42;

    var validationValue = new AtomicBoolean(true);

    var impl =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(
                Map.of(),
                new IdGeneratorSource() {
                  @Override
                  public int nodeId() {
                    return validationValue.get() ? nodeId : -1;
                  }

                  @Override
                  public long currentTimeMillis() {
                    return clock.currentTimeMillis();
                  }
                });

    soft.assertThatCode(impl::generateId).doesNotThrowAnyException();

    validationValue.set(false);

    soft.assertThatIllegalStateException()
        .isThrownBy(impl::generateId)
        .withMessage("Cannot generate a new ID, shutting down?");
  }

  static IdGeneratorSource idGeneratorSource(int nodeId, LongSupplier clock) {
    return new IdGeneratorSource() {
      @Override
      public int nodeId() {
        return nodeId;
      }

      @Override
      public long currentTimeMillis() {
        return clock.getAsLong();
      }
    };
  }
}
