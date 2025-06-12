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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.polaris.core.entity.PolarisEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class FileFlushTaskTest {
  @Mock private Consumer<String> cleanupFutures;
  @Mock private BiConsumer<String, List<PolarisEvent>> eventWriter;
  private FileFlushTask fileFlushTask;
  private static final String testFile = "test-file";
  private static final String testRealm = "test-realm";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    fileFlushTask = new FileFlushTask(testFile, testRealm, cleanupFutures, eventWriter);
  }

  @Test
  void testRunFileNotFound() throws Exception {
    FileFlushTask task = spy(fileFlushTask);
    doThrow(new FileNotFoundException()).when(task).createInputStream(anyString());
    task.run();
    verify(cleanupFutures, times(1)).accept(testFile);
    verify(eventWriter, never()).accept(any(), any());
  }

  @Test
  void testRunKryoExceptionEOFPrematurely() throws Exception {
    FileFlushTask task = spy(fileFlushTask);
    Input input = mock(Input.class);
    doReturn(input).when(task).createInputStream(anyString());
    when(input.available()).thenReturn(1).thenReturn(0);
    KryoException kryoException = new KryoException(new EOFException());
    doThrow(kryoException).when(task).readPolarisEvent(any(Input.class));
    task.run();
    verify(cleanupFutures, times(1)).accept(testFile);
    verify(eventWriter, never()).accept(any(), any());
  }

  @Test
  void testRunKryoExceptionOther() throws Exception {
    FileFlushTask task = spy(fileFlushTask);
    Input input = mock(Input.class);
    doReturn(input).when(task).createInputStream(anyString());
    KryoException kryoException = new KryoException("error");
    doThrow(kryoException).when(task).readPolarisEvent(any(Input.class));
    task.run();
    verify(cleanupFutures, times(1)).accept(testFile);
    verify(eventWriter, never()).accept(any(), any());
  }

  @Test
  void testRunSuccessfulRead() throws Exception {
    FileFlushTask task = spy(fileFlushTask);
    Input input = mock(Input.class);
    PolarisEvent event1 = mock(PolarisEvent.class);
    PolarisEvent event2 = mock(PolarisEvent.class);
    doReturn(input).when(task).createInputStream(anyString());
    when(task.readPolarisEvent(input))
        .thenReturn(event1)
        .thenReturn(event2)
        .thenThrow(new KryoException(new EOFException()));
    task.run();
    ArgumentCaptor<List<PolarisEvent>> captor = ArgumentCaptor.forClass(List.class);
    verify(eventWriter).accept(eq(testRealm), captor.capture());
    List<PolarisEvent> events = captor.getValue();
    assertEquals(2, events.size());
    assertTrue(events.contains(event1));
    assertTrue(events.contains(event2));
    verify(cleanupFutures, times(1)).accept(testFile);
  }

  @Test
  void testEventWriterError() throws Exception {
    FileFlushTask task = spy(fileFlushTask);
    Input input = mock(Input.class);
    PolarisEvent event = mock(PolarisEvent.class);
    doReturn(input).when(task).createInputStream(anyString());
    when(task.readPolarisEvent(input))
        .thenReturn(event)
        .thenThrow(new KryoException(new EOFException()));
    doThrow(new RuntimeException("Event writer error"))
        .when(eventWriter)
        .accept(anyString(), anyList());

    task.run();

    // Verify cleanup is not called since the file is left for future cleanup tasks
    verify(cleanupFutures, never()).accept(testFile);
    // Verify eventWriter was called and threw an exception
    verify(eventWriter, times(1)).accept(eq(testRealm), anyList());
  }
}
