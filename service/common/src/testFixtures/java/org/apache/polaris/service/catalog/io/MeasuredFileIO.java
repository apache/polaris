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

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * File IO wrapper used for tests. It measures the number of bytes read, files written, and files
 * deleted. It can inject exceptions during InputFile and OutputFile creation.
 */
public class MeasuredFileIO implements FileIO {
  private final FileIO io;

  // When present, the following will be used to throw exceptions at various parts of the IO
  private final Optional<Supplier<RuntimeException>> newInputFileExceptionSupplier;
  private final Optional<Supplier<RuntimeException>> newOutputFileExceptionSupplier;
  private final Optional<Supplier<RuntimeException>> getLengthExceptionSupplier;

  private long inputBytes;
  private int numOutputFiles;
  private int numDeletedFiles;

  public MeasuredFileIO(
      FileIO io,
      Optional<Supplier<RuntimeException>> newInputFileExceptionSupplier,
      Optional<Supplier<RuntimeException>> newOutputFileExceptionSupplier,
      Optional<Supplier<RuntimeException>> getLengthExceptionSupplier) {
    this.io = io;
    this.newInputFileExceptionSupplier = newInputFileExceptionSupplier;
    this.newOutputFileExceptionSupplier = newOutputFileExceptionSupplier;
    this.getLengthExceptionSupplier = getLengthExceptionSupplier;
  }

  public long getInputBytes() {
    return inputBytes;
  }

  public int getNumOuptutFiles() {
    return numOutputFiles;
  }

  public int getNumDeletedFiles() {
    return numDeletedFiles;
  }

  private InputFile wrapInputFile(InputFile inner) {
    newInputFileExceptionSupplier.ifPresent(
        s -> {
          throw s.get();
        });

    // Use the inner's length in case the MeasuredInputFile throws a getLength exception
    inputBytes += inner.getLength();
    return new MeasuredInputFile(inner, getLengthExceptionSupplier);
  }

  @Override
  public InputFile newInputFile(String path) {
    return wrapInputFile(io.newInputFile(path));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return wrapInputFile(io.newInputFile(path, length));
  }

  @Override
  public InputFile newInputFile(DataFile file) {
    return wrapInputFile(io.newInputFile(file));
  }

  @Override
  public InputFile newInputFile(DeleteFile file) {
    return wrapInputFile(io.newInputFile(file));
  }

  @Override
  public InputFile newInputFile(ManifestFile manifest) {
    return wrapInputFile(io.newInputFile(manifest));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    newOutputFileExceptionSupplier.ifPresent(
        s -> {
          throw s.get();
        });

    numOutputFiles++;
    return io.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    numDeletedFiles++;
    io.deleteFile(path);
  }

  @Override
  public void deleteFile(InputFile file) {
    numDeletedFiles++;
    io.deleteFile(file);
  }

  @Override
  public void deleteFile(OutputFile file) {
    numDeletedFiles++;
    io.deleteFile(file);
  }

  @Override
  public Map<String, String> properties() {
    return io.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    io.initialize(properties);
  }

  @Override
  public void close() {
    io.close();
  }
}
