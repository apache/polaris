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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.polaris.core.entity.PolarisEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.threeten.extra.MutableClock;

class FileBufferListingTaskTest {
  @Mock private Function<FileBufferListingTask.TaskSubmissionInput, Future> taskSubmitter;
  @Mock private BiConsumer<String, Integer> rotateShard;
  @Mock private BiConsumer<String, List<PolarisEvent>> eventWriter;
  private ConcurrentHashMap<String, Future> activeFlushFutures = new ConcurrentHashMap<>();
  private FileBufferListingTask task;
  private static final int TIME_TO_FLUSH = 60000;
  private static final String BUFFER_DIR = "/mock/buffer/directory";
  private static final MutableClock clock = MutableClock.of(Instant.now(), ZoneOffset.UTC);

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    task =
        new FileBufferListingTask(
            BUFFER_DIR,
            taskSubmitter,
            activeFlushFutures,
            TIME_TO_FLUSH,
            clock,
            rotateShard,
            eventWriter);
  }

  @Test
  void testNoRealmDirs() {
    File mockTopLevel = mock(File.class);
    when(mockTopLevel.listFiles()).thenReturn(null);
    FileBufferListingTask t = spy(task);
    doReturn(mockTopLevel).when(t).newFile(BUFFER_DIR);
    t.run();
    // Only the rescheduling of FileBufferListingTask itself should occur
    verify(taskSubmitter, times(1))
        .apply(argThat(input -> input.task() instanceof FileBufferListingTask));
  }

  @Test
  void testEmptyRealmDirsLengthZero() {
    File mockTopLevel = mock(File.class);
    when(mockTopLevel.listFiles()).thenReturn(new File[0]);
    FileBufferListingTask t = spy(task);
    doReturn(mockTopLevel).when(t).newFile(BUFFER_DIR);
    t.run();
    // Only the rescheduling of FileBufferListingTask itself should occur
    verify(taskSubmitter, times(1))
        .apply(argThat(input -> input.task() instanceof FileBufferListingTask));
  }

  @Test
  void testRealmFoundWithNoBufferDirs() {
    File mockTopLevel = mock(File.class);
    File mockRealm = mock(File.class);
    when(mockTopLevel.listFiles()).thenReturn(new File[] {mockRealm});
    when(mockRealm.getName()).thenReturn("realm1");
    when(mockRealm.listFiles()).thenReturn(null); // No bufferDirs
    FileBufferListingTask t = spy(task);
    doReturn(mockTopLevel).when(t).newFile(BUFFER_DIR);
    t.run();
    // Only the rescheduling of FileBufferListingTask itself should occur
    verify(taskSubmitter, times(1))
        .apply(argThat(input -> input.task() instanceof FileBufferListingTask));
    verify(taskSubmitter, times(0)).apply(argThat(input -> input.task() instanceof FileFlushTask));
  }

  @Test
  void testHappyPath() {
    File mockTopLevel = mock(File.class);

    // Setup directory structure
    File mockRealm1 = setupShardWithTwoFiles("realm1");
    File mockRealm2 = setupShardWithTwoFiles("realm2");
    when(mockTopLevel.listFiles()).thenReturn(new File[] {mockRealm1, mockRealm2});

    FileBufferListingTask t = spy(task);
    doReturn(mockTopLevel).when(t).newFile(BUFFER_DIR);
    t.run();

    // Should reschedule itself
    verify(taskSubmitter, times(1))
        .apply(argThat(input -> input.task() instanceof FileBufferListingTask));
    // Should submit exactly 4 FileFlushTask (one for each shard's non-latest file; 2 realms with 2
    // files each)
    verify(taskSubmitter, times(4)).apply(argThat(input -> input.task() instanceof FileFlushTask));
  }

  private File setupShardWithTwoFiles(String realmName) {
    String shardName1 = "polaris-event-buffer-shard-1";
    String shardName2 = "polaris-event-buffer-shard-2";
    long now = clock.millis();
    long mockShard1File1Timestamp = now - 75000; // larger than TIME_TO_FLUSH
    long mockShard1File2Timestamp = now - 5000; // smaller than TIME_TO_FLUSH
    long mockShard2File1Timestamp = now - 10000; // smaller than TIME_TO_FLUSH
    long mockShard2File2Timestamp = now - 100000; // larger than TIME_TO_FLUSH

    File mockRealm = mock(File.class);
    File mockShard1 = mock(File.class);
    File mockShard2 = mock(File.class);
    File mockShard1File1 = mock(File.class);
    File mockShard1File2 = mock(File.class);
    File mockShard2File1 = mock(File.class);
    File mockShard2File2 = mock(File.class);

    when(mockRealm.getName()).thenReturn(realmName);
    when(mockRealm.listFiles()).thenReturn(new File[] {mockShard1, mockShard2});
    // Shard 1
    when(mockShard1.getName()).thenReturn(shardName1);
    when(mockShard1.listFiles()).thenReturn(new File[] {mockShard1File1, mockShard1File2});
    when(mockShard1File1.getName()).thenReturn("buffer-" + mockShard1File1Timestamp);
    when(mockShard1File2.getName()).thenReturn("buffer-" + mockShard1File2Timestamp);
    when(mockShard1File1.getAbsolutePath())
        .thenReturn(
            BUFFER_DIR
                + "/"
                + realmName
                + "/"
                + shardName1
                + "/"
                + "buffer-"
                + mockShard1File1Timestamp);
    when(mockShard1File2.getAbsolutePath())
        .thenReturn(
            BUFFER_DIR
                + "/"
                + realmName
                + "/"
                + shardName1
                + "/"
                + "buffer-"
                + mockShard1File2Timestamp);
    when(mockShard1File1.compareTo(mockShard1File2)).thenReturn(-1);
    when(mockShard1File2.compareTo(mockShard1File1)).thenReturn(1);
    // Shard 2
    when(mockShard2.getName()).thenReturn(shardName2);
    when(mockShard2.listFiles()).thenReturn(new File[] {mockShard2File1, mockShard2File2});
    when(mockShard2File1.getName()).thenReturn("buffer-" + mockShard2File1Timestamp);
    when(mockShard2File2.getName()).thenReturn("buffer-" + mockShard2File2Timestamp);
    when(mockShard2File1.getAbsolutePath())
        .thenReturn(
            BUFFER_DIR
                + "/"
                + realmName
                + "/"
                + shardName2
                + "/"
                + "buffer-"
                + mockShard2File1Timestamp);
    when(mockShard2File2.getAbsolutePath())
        .thenReturn(
            BUFFER_DIR
                + "/"
                + realmName
                + "/"
                + shardName2
                + "/"
                + "buffer-"
                + mockShard2File2Timestamp);
    when(mockShard2File1.compareTo(mockShard2File2)).thenReturn(1);
    when(mockShard2File2.compareTo(mockShard2File1)).thenReturn(-1);
    return mockRealm;
  }
}
