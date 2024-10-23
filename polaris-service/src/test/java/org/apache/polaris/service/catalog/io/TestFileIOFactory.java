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

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

/**
 * A FileIOFactory that measures the number of bytes read, files written, and files deleted. It can
 * inject exceptions at various parts of the IO construction.
 */
@JsonTypeName("test")
public class TestFileIOFactory implements FileIOFactory {
  private final List<TestFileIO> ios = new ArrayList<>();
  public @NotNull Supplier<RuntimeException> loadFileIOExceptionSupplier = () -> null;
  public @NotNull Supplier<RuntimeException> newInputFileExceptionSupplier = () -> null;
  public @NotNull Supplier<RuntimeException> newOutputFileExceptionSupplier = () -> null;

  public TestFileIOFactory() {}

  @Override
  public FileIO loadFileIO(String ioImpl, Map<String, String> properties) {
    RuntimeException e = loadFileIOExceptionSupplier.get();
    if (e != null) {
      throw e;
    }

    TestFileIO wrapped =
        new TestFileIO(
            CatalogUtil.loadFileIO(ioImpl, properties, new Configuration()),
            newInputFileExceptionSupplier,
            newOutputFileExceptionSupplier);
    ios.add(wrapped);
    return wrapped;
  }

  public long getInputBytes() {
    long sum = 0;
    for (TestFileIO io : ios) {
      sum += io.getInputBytes();
    }
    return sum;
  }

  public long getNumOutputFiles() {
    long sum = 0;
    for (TestFileIO io : ios) {
      sum += io.getNumOuptutFiles();
    }
    return sum;
  }

  public long getNumDeletedFiles() {
    long sum = 0;
    for (TestFileIO io : ios) {
      sum += io.getNumDeletedFiles();
    }
    return sum;
  }
}
