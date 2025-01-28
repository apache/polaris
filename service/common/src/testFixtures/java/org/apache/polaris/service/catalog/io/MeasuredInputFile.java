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

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/** An InputFile wrapper that can be forced to throw exceptions. */
public class MeasuredInputFile implements InputFile {
  private final InputFile inputFile;
  private final Optional<Supplier<RuntimeException>> getLengthExceptionSupplier;

  public MeasuredInputFile(
      InputFile inputFile, Optional<Supplier<RuntimeException>> getLengthExceptionSupplier) {
    this.inputFile = inputFile;
    this.getLengthExceptionSupplier = getLengthExceptionSupplier;
  }

  @Override
  public long getLength() {
    getLengthExceptionSupplier.ifPresent(
        s -> {
          throw s.get();
        });

    return inputFile.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return inputFile.newStream();
  }

  @Override
  public String location() {
    return inputFile.location();
  }

  @Override
  public boolean exists() {
    return inputFile.exists();
  }
}
