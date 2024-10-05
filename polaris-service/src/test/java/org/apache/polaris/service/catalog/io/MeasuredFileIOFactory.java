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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/** A FileIOFactory that measures the number of bytes read, files written, and files deleted. */
@JsonTypeName("measured")
public class MeasuredFileIOFactory implements FileIOFactory {
  private final List<MeasuredFileIO> ios;

  public MeasuredFileIOFactory() {
    ios = new ArrayList<>();
  }

  @Override
  public FileIO loadFileIO(String ioImpl, Map<String, String> properties) {
    MeasuredFileIO wrapped =
        new MeasuredFileIO(CatalogUtil.loadFileIO(ioImpl, properties, new Configuration()));
    ios.add(wrapped);
    return wrapped;
  }

  public long getInputBytes() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.inputBytes;
    }
    return sum;
  }

  public long getNumOutputFiles() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.numOutputFiles;
    }
    return sum;
  }

  public long getNumDeletedFiles() {
    long sum = 0;
    for (MeasuredFileIO io : ios) {
      sum += io.numDeletedFiles;
    }
    return sum;
  }

  public static class MeasuredFileIO implements FileIO {
    private final FileIO io;
    private long inputBytes;
    private int numOutputFiles;
    private int numDeletedFiles;

    public MeasuredFileIO(FileIO io) {
      this.io = io;
    }

    private InputFile measureInputFile(InputFile inner) {
      inputBytes += inner.getLength();
      return inner;
    }

    @Override
    public InputFile newInputFile(String path) {
      return measureInputFile(io.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return measureInputFile(io.newInputFile(path, length));
    }

    @Override
    public InputFile newInputFile(DataFile file) {
      return measureInputFile(io.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
      return measureInputFile(io.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest) {
      return measureInputFile(io.newInputFile(manifest));
    }

    @Override
    public OutputFile newOutputFile(String path) {
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
}
