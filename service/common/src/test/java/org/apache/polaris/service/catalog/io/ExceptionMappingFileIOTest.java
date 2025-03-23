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
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for ExceptionMappingFileIO */
public class ExceptionMappingFileIOTest {
  private static final String PATH = "x/y/z";
  private static final Map<String, String> PROPERTIES = Map.of("k", "v");

  @Test
  void testProxiesMethodCalls() {
    try (var io = Mockito.mock(FileIO.class)) {
      var wrappedIO = new ExceptionMappingFileIO(io);

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

      var wrappedIO = new ExceptionMappingFileIO(io);
      Assertions.assertThrows(FileIOUnknownHostException.class, () -> wrappedIO.newInputFile(PATH));
    }
  }
}
