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
package org.apache.polaris.service.catalog.io;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Unit tests for WasbTranslatingFileIO */
public class WasbTranslatingFileIOTest {
  private static final String WASB_PATH =
      "wasb://container@storageaccount.blob.core.windows.net/f1";
  private static final String ABFS_PATH =
      "abfs://container@storageaccount.blob.core.windows.net/f1";
  private static final String OTHER_WASB_PATH =
      "wasb://container@storageaccount.blob.core.windows.net/f2";
  private static final String OTHER_ABFS_PATH =
      "abfs://container@storageaccount.blob.core.windows.net/f2";
  private static final String NON_AZURE_PATH = "s3://bucket/key";

  /** A FileIO that also supports bulk operations, as ADLSFileIO does. */
  private interface BulkFileIO extends SupportsBulkOperations {}

  @Test
  void testAdvertisesBulkSupportOnlyWhenTheWrappedIoHasIt() {
    try (var bulkIo = Mockito.mock(BulkFileIO.class)) {
      Assertions.assertThat(WasbTranslatingFileIO.wrap(bulkIo))
          .isInstanceOf(SupportsBulkOperations.class);
    }
    try (var plainIo = Mockito.mock(FileIO.class)) {
      Assertions.assertThat(WasbTranslatingFileIO.wrap(plainIo))
          .isNotInstanceOf(SupportsBulkOperations.class);
    }
  }

  @Test
  void testDeleteFileTranslatesWasbToAbfs() {
    try (var io = Mockito.mock(FileIO.class)) {
      WasbTranslatingFileIO.wrap(io).deleteFile(WASB_PATH);

      Mockito.verify(io, Mockito.times(1)).deleteFile(ABFS_PATH);
    }
  }

  @Test
  void testDeleteFilesTranslatesEveryPath() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      var wrappedIO = (SupportsBulkOperations) WasbTranslatingFileIO.wrap(io);

      wrappedIO.deleteFiles(List.of(WASB_PATH, OTHER_WASB_PATH, NON_AZURE_PATH));

      ArgumentCaptor<Iterable<String>> captor = ArgumentCaptor.captor();
      Mockito.verify(io, Mockito.times(1)).deleteFiles(captor.capture());
      Assertions.assertThat(captor.getValue())
          .containsExactly(ABFS_PATH, OTHER_ABFS_PATH, NON_AZURE_PATH);
    }
  }

  @Test
  void testDeleteFilesArgumentCanBeIteratedMoreThanOnce() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      var wrappedIO = (SupportsBulkOperations) WasbTranslatingFileIO.wrap(io);

      wrappedIO.deleteFiles(List.of(WASB_PATH));

      ArgumentCaptor<Iterable<String>> captor = ArgumentCaptor.captor();
      Mockito.verify(io).deleteFiles(captor.capture());

      // an implementation may size the batch before deleting, so a one-shot iterator would
      // silently delete nothing on the second pass
      var first = new ArrayList<String>();
      captor.getValue().forEach(first::add);
      var second = new ArrayList<String>();
      captor.getValue().forEach(second::add);
      Assertions.assertThat(first).containsExactly(ABFS_PATH);
      Assertions.assertThat(second).isEqualTo(first);
    }
  }

  @Test
  void testDeleteFilesPropagatesBulkDeletionFailures() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      Mockito.doThrow(new BulkDeletionFailureException(1)).when(io).deleteFiles(Mockito.any());

      var wrappedIO = (SupportsBulkOperations) WasbTranslatingFileIO.wrap(io);
      Assertions.assertThatThrownBy(() -> wrappedIO.deleteFiles(List.of(WASB_PATH)))
          .isInstanceOf(BulkDeletionFailureException.class);
    }
  }

  @Test
  void testWrappingAnExceptionMappingFileIoKeepsBulkSupport() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      // the production stack: WasbTranslatingFileIOFactory wraps what DefaultFileIOFactory returns
      var wrappedIO = WasbTranslatingFileIO.wrap(ExceptionMappingFileIO.wrap(io));
      Assertions.assertThat(wrappedIO).isInstanceOf(SupportsBulkOperations.class);

      ((SupportsBulkOperations) wrappedIO).deleteFiles(List.of(WASB_PATH, NON_AZURE_PATH));

      ArgumentCaptor<Iterable<String>> captor = ArgumentCaptor.captor();
      Mockito.verify(io, Mockito.times(1)).deleteFiles(captor.capture());
      Assertions.assertThat(captor.getValue()).containsExactly(ABFS_PATH, NON_AZURE_PATH);
    }
  }
}
