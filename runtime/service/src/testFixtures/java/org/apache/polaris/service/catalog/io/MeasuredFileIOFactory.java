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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.inject.Vetoed;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;

/**
 * A FileIOFactory that measures the number of bytes read, files written, and files deleted. It can
 * inject exceptions at various parts of the IO construction.
 */
@Vetoed
public class MeasuredFileIOFactory implements FileIOFactory {
  private final List<MeasuredFileIO> ios = new ArrayList<>();

  // When present, the following will be used to throw exceptions at various parts of the IO
  public Optional<Supplier<RuntimeException>> loadFileIOExceptionSupplier = Optional.empty();
  public Optional<Supplier<RuntimeException>> newInputFileExceptionSupplier = Optional.empty();
  public Optional<Supplier<RuntimeException>> newOutputFileExceptionSupplier = Optional.empty();
  public Optional<Supplier<RuntimeException>> getLengthExceptionSupplier = Optional.empty();

  private final FileIOFactory defaultFileIOFactory;

  @Inject
  public MeasuredFileIOFactory(StorageAccessConfigProvider storageAccessConfigProvider) {
    defaultFileIOFactory = new DefaultFileIOFactory(storageAccessConfigProvider);
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
    loadFileIOExceptionSupplier.ifPresent(
        s -> {
          throw s.get();
        });

    MeasuredFileIO wrapped =
        new MeasuredFileIO(
            defaultFileIOFactory.loadFileIO(
                callContext,
                ioImplClassName,
                properties,
                identifier,
                tableLocations,
                storageActions,
                resolvedEntityPath),
            newInputFileExceptionSupplier,
            newOutputFileExceptionSupplier,
            getLengthExceptionSupplier);
    ios.add(wrapped);
    return wrapped;
  }

  public long getInputBytes() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.getInputBytes();
    }
    return sum;
  }

  public long getNumOutputFiles() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.getNumOuptutFiles();
    }
    return sum;
  }

  public long getNumDeletedFiles() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.getNumDeletedFiles();
    }
    return sum;
  }
}
