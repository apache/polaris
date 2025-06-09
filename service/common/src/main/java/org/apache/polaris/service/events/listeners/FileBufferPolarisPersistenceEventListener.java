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
package org.apache.polaris.service.events.listeners;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-file-buffer")
public class FileBufferPolarisPersistenceEventListener extends PolarisPersistenceEventListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileBufferPolarisPersistenceEventListener.class);
  MetaStoreManagerFactory metaStoreManagerFactory;
  PolarisConfigurationStore polarisConfigurationStore;

  // Key: str - Realm
  // Value:
  //    Key: int - shard number
  //    Value: BufferShard - an object representing the directory and file where events are
  // persisted on the filesystem
  private final HashMap<String, HashMap<Integer, BufferShard>> buffers = new HashMap<>();
  ConcurrentHashMap<String, Future> activeFlushFutures = new ConcurrentHashMap<>();

  ScheduledExecutorService threadPool;
  private static int shardCount;
  private static int maxBufferSize;
  private final Kryo kryo = new Kryo();
  private static final String BUFFER_SHARD_PREFIX = "polaris-event-buffer-shard-";

  @Inject
  public FileBufferPolarisPersistenceEventListener(
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore polarisConfigurationStore) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.polarisConfigurationStore = polarisConfigurationStore;
    shardCount =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_NUM_SHARDS);
    maxBufferSize =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_MAX_SIZE);
    int timeToFlush =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS);
    int numThreads =
        polarisConfigurationStore.getConfiguration(
            null, FeatureConfiguration.EVENT_BUFFER_NUM_THREADS);
    threadPool = Executors.newScheduledThreadPool(numThreads);
    kryo.register(PolarisEvent.class);
    kryo.register(PolarisEvent.ResourceType.class);

    // Start BufferListingTask
    Function<FileBufferListingTask.TaskSubmissionInput, Future> taskSubmissionFunction =
        input -> threadPool.schedule(input.task(), input.delayInMs(), TimeUnit.MILLISECONDS);
    BiConsumer<String, List<PolarisEvent>> eventWriter =
        (realmId, polarisEvents) -> getBasePersistenceInstance(realmId).writeEvents(polarisEvents);
    var future =
        threadPool.schedule(
            new FileBufferListingTask(
                getBufferDirectory(),
                taskSubmissionFunction,
                activeFlushFutures,
                timeToFlush,
                this::rotateShard,
                eventWriter),
            timeToFlush,
            TimeUnit.MILLISECONDS);
  }

  @Override
  void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
    int shardNum = polarisEvent.hashCode() % shardCount;
    String realmId = callCtx.getRealmContext().getRealmIdentifier();
    if (!buffers.containsKey(realmId)) {
      createBuffersForRealm(realmId);
    }
    BufferShard bufferShard = buffers.get(realmId).get(shardNum);
    if (bufferShard == null) {
      LOGGER.error(
          "No buffer shard found for realm: #{}, shard #{}. Event dropped: {}",
          realmId,
          shardNum,
          polarisEvent);
      return;
    }

    kryo.writeObject(bufferShard.output, polarisEvent);
    bufferShard.output.flush();

    // If too many events in this buffer shard, start a new shard
    int bufferEventCount = bufferShard.eventCount.getAndIncrement() + 1;
    if (bufferEventCount >= maxBufferSize) {
      rotateShard(realmId, shardNum);
    }
  }

  private void createBuffersForRealm(String realmId) {
    HashMap<Integer, BufferShard> bufferShardsForRealm = new HashMap<>();
    buffers.put(realmId, bufferShardsForRealm);
    for (int i = 0; i < shardCount; i++) {
      bufferShardsForRealm.put(i, createShard(realmId, i));
    }
  }

  private BufferShard createShard(String realmId, int shardNum) {
    String bufferDirName =
        getBufferDirectory() + realmId + "/" + BUFFER_SHARD_PREFIX + shardNum + "/";
    File file = new File(bufferDirName + "buffer_timestamp-" + System.currentTimeMillis());
    File parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs(); // Creates all missing parent directories
    }
    try {
      file.createNewFile();
      Output output = new Output(new FileOutputStream(file));
      return new BufferShard(file, output, new AtomicInteger(0));
    } catch (IOException e) {
      LOGGER.error("Buffer shard {} was unable to initialize: {}", shardNum, e);
    }
    return null;
  }

  private void rotateShard(String realmId, int shardNum) {
    buffers.get(realmId).put(shardNum, createShard(realmId, shardNum));
  }

  private BasePersistence getBasePersistenceInstance(String realmId) {
    return metaStoreManagerFactory.getOrCreateSessionSupplier(getRealmContext(realmId)).get();
  }

  public String getBufferDirectory() {
    return System.getProperty("java.io.tmpdir") + "event_buffers/";
  }

  private RealmContext getRealmContext(String realmId) {
    return () -> realmId;
  }

  private record BufferShard(File shardFile, Output output, AtomicInteger eventCount) {}
}
