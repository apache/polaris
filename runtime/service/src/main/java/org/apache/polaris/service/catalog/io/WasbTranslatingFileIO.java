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
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.azure.AzureLocation;

/**
 * A {@link FileIO} implementation that translates WASB paths into ABFS paths and then delegates to
 * another underlying FileIO implementation
 *
 * <p>Instances are created with {@link #wrap(FileIO)}, which preserves the wrapped FileIO's {@link
 * SupportsBulkOperations} capability. Callers discover that capability with {@code instanceof}, and
 * a plain wrapper would hide it.
 */
public class WasbTranslatingFileIO implements FileIO {
  private final FileIO io;

  private static final String WASB_SCHEME = "wasb";
  private static final String ABFS_SCHEME = "abfs";

  private WasbTranslatingFileIO(FileIO io) {
    this.io = io;
  }

  public static WasbTranslatingFileIO wrap(FileIO io) {
    return io instanceof SupportsBulkOperations
        ? new BulkWasbTranslatingFileIO(io)
        : new WasbTranslatingFileIO(io);
  }

  private static String translate(String path) {
    if (path == null) {
      return null;
    } else {
      StorageLocation storageLocation = StorageLocation.of(path);
      if (storageLocation instanceof AzureLocation azureLocation) {
        String scheme = azureLocation.getScheme();
        if (scheme.startsWith(WASB_SCHEME)) {
          scheme = scheme.replaceFirst(WASB_SCHEME, ABFS_SCHEME);
        }
        return String.format(
            "%s://%s@%s.%s/%s",
            scheme,
            azureLocation.getContainer(),
            azureLocation.getStorageAccount(),
            azureLocation.getEndpoint(),
            azureLocation.getFilePath());
      } else {
        return path;
      }
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    return io.newInputFile(translate(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return io.newOutputFile(translate(path));
  }

  @Override
  public void deleteFile(String path) {
    io.deleteFile(translate(path));
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

  private static class BulkWasbTranslatingFileIO extends WasbTranslatingFileIO
      implements SupportsBulkOperations {

    private final SupportsBulkOperations bulkIo;

    private BulkWasbTranslatingFileIO(FileIO io) {
      super(io);
      this.bulkIo = (SupportsBulkOperations) io;
    }

    @Override
    public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
      bulkIo.deleteFiles(
          () ->
              StreamSupport.stream(paths.spliterator(), false)
                  .map(WasbTranslatingFileIO::translate)
                  .iterator());
    }
  }
}
