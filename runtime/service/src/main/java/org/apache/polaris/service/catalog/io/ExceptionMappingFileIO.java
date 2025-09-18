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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.net.UnknownHostException;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;

/** A {@link FileIO} implementation that wraps an existing FileIO and re-maps exceptions */
public class ExceptionMappingFileIO implements FileIO {
  private final FileIO io;

  public ExceptionMappingFileIO(FileIO io) {
    this.io = io;
  }

  private void handleException(RuntimeException e) {
    for (Throwable t : Throwables.getCausalChain(e)) {
      // UnknownHostException isn't a RuntimeException so it's always wrapped
      if (t instanceof UnknownHostException) {
        throw new FileIOUnknownHostException("UnknownHostException during File IO", t);
      }
    }
  }

  @VisibleForTesting
  public FileIO getInnerIo() {
    return io;
  }

  @Override
  public InputFile newInputFile(String path) {
    try {
      return io.newInputFile(path);
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }

  @Override
  public OutputFile newOutputFile(String path) {
    try {
      return io.newOutputFile(path);
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }

  @Override
  public void deleteFile(String path) {
    try {
      io.deleteFile(path);
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }

  @Override
  public Map<String, String> properties() {
    try {
      return io.properties();
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {
    try {
      io.initialize(properties);
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      io.close();
    } catch (RuntimeException e) {
      handleException(e);
      throw e;
    }
  }
}
