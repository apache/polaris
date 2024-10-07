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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.azure.AzureLocation;

/** A FileIOFactory that translates WASB paths to ABFS ones */
@JsonTypeName("wasb")
public class WasbTranslatingFileIOFactory implements FileIOFactory {
  private final List<FileIO> ios;

  public WasbTranslatingFileIOFactory() {
    ios = new ArrayList<>();
  }

  @Override
  public FileIO loadFileIO(String ioImpl, Map<String, String> properties) {
    WasbTranslatingFileIO wrapped =
        new WasbTranslatingFileIO(CatalogUtil.loadFileIO(ioImpl, properties, new Configuration()));
    ios.add(wrapped);
    return wrapped;
  }

  public static class WasbTranslatingFileIO implements FileIO {
    private final FileIO io;

    private static final String WASB_SCHEME = "wasb";
    private static final String ABFS_SCHEME = "abfs";

    public WasbTranslatingFileIO(FileIO io) {
      this.io = io;
    }

    private static String translate(String path) {
      if (path == null) {
        return null;
      } else {
        StorageLocation storageLocation = StorageLocation.of(path);
        if (storageLocation instanceof AzureLocation azureLocation) {
          String scheme = azureLocation.getScheme();
          if (scheme.startsWith(WASB_SCHEME)) {
            scheme = scheme.replaceAll(WASB_SCHEME, ABFS_SCHEME);
          }
          return String.format(
              "%s://%s@%s.dfs.core.windows.net/%s",
              scheme,
              azureLocation.getContainer(),
              azureLocation.getStorageAccount(),
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
  }
}
