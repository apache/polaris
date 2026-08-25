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

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for ExceptionMappingFileIO */
public class ExceptionMappingFileIOTest {
  private static final String PATH = "x/y/z";
  private static final Map<String, String> PROPERTIES = Map.of("k", "v");

  @Test
  void testProxiesMethodCalls() {
    try (var io = Mockito.mock(FileIO.class)) {
      var wrappedIO = ExceptionMappingFileIO.wrap(io);

      wrappedIO.newInputFile(PATH);
      Mockito.verify(io, Mockito.times(1)).newInputFile(PATH);

      wrappedIO.newOutputFile(PATH);
      Mockito.verify(io, Mockito.times(1)).newOutputFile(PATH);

      wrappedIO.deleteFile(PATH);
      Mockito.verify(io, Mockito.times(1)).deleteFile(PATH);

      wrappedIO.properties();
      Mockito.verify(io, Mockito.times(1)).properties();

      wrappedIO.initialize(PROPERTIES);
      Mockito.verify(io, Mockito.times(1)).initialize(PROPERTIES);

      wrappedIO.close();
      Mockito.verify(io, Mockito.times(1)).close();
    }
  }

  @Test
  void testExceptionRemapping() {
    try (var io = Mockito.mock(FileIO.class)) {
      Mockito.doThrow(new RuntimeException(new UnknownHostException())).when(io).newInputFile(PATH);

      var wrappedIO = ExceptionMappingFileIO.wrap(io);
      Assertions.assertThatThrownBy(() -> wrappedIO.newInputFile(PATH))
          .isInstanceOf(FileIOUnknownHostException.class);
    }
  }

  /** A FileIO that also supports bulk operations, as S3FileIO, GCSFileIO and ADLSFileIO all do. */
  private interface BulkFileIO extends SupportsBulkOperations {}

  @Test
  void testAdvertisesBulkSupportOnlyWhenTheWrappedIoHasIt() {
    try (var bulkIo = Mockito.mock(BulkFileIO.class)) {
      Assertions.assertThat(ExceptionMappingFileIO.wrap(bulkIo))
          .isInstanceOf(SupportsBulkOperations.class);
    }
    try (var plainIo = Mockito.mock(FileIO.class)) {
      Assertions.assertThat(ExceptionMappingFileIO.wrap(plainIo))
          .isNotInstanceOf(SupportsBulkOperations.class);
    }
  }

  @Test
  void testDeleteFilesDelegatesToTheWrappedIo() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      var paths = List.of(PATH, "a/b/c");

      ((SupportsBulkOperations) ExceptionMappingFileIO.wrap(io)).deleteFiles(paths);

      Mockito.verify(io, Mockito.times(1)).deleteFiles(paths);
      Mockito.verify(io, Mockito.never()).deleteFile(Mockito.anyString());
    }
  }

  @Test
  void testDeleteFilesRemapsExceptions() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      var paths = List.of(PATH);
      Mockito.doThrow(new RuntimeException(new UnknownHostException())).when(io).deleteFiles(paths);

      var wrappedIO = (SupportsBulkOperations) ExceptionMappingFileIO.wrap(io);
      Assertions.assertThatThrownBy(() -> wrappedIO.deleteFiles(paths))
          .isInstanceOf(FileIOUnknownHostException.class);
    }
  }

  @Test
  void testDeleteFilesRethrowsUnmappedExceptionsUnchanged() {
    try (var io = Mockito.mock(BulkFileIO.class)) {
      var paths = List.of(PATH);
      var failure = new IllegalStateException("boom");
      Mockito.doThrow(failure).when(io).deleteFiles(paths);

      var wrappedIO = (SupportsBulkOperations) ExceptionMappingFileIO.wrap(io);
      Assertions.assertThatThrownBy(() -> wrappedIO.deleteFiles(paths)).isSameAs(failure);
    }
  }
}
