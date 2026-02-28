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
package org.apache.polaris.storage.files.impl;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergBridge;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;

public class IcebergFixtures {
  public final Schema schema;
  public final PartitionSpec spec;
  public final TableMetadata tableMetadata;
  public final String tableMetadataString;
  public final byte[] tableMetadataBytes;

  public final String prefix;
  public final int numSnapshots;
  public final int numManifestFiles;
  public final int numDataFiles;

  public IcebergFixtures(String prefix, int numSnapshots, int numManifestFiles, int numDataFiles) {
    this.prefix = prefix;
    this.numSnapshots = numSnapshots;
    this.numManifestFiles = numManifestFiles;
    this.numDataFiles = numDataFiles;

    schema = new Schema(1, Types.NestedField.required(1, "foo", Types.StringType.get()));
    spec = PartitionSpec.unpartitioned();

    var tableMetadataBuilder =
        TableMetadata.buildFrom(
            TableMetadata.newTableMetadata(schema, spec, prefix, Map.of()).withUUID());
    for (var snapshotId = 1; snapshotId <= numSnapshots; snapshotId++) {
      var manifestList = manifestListPath(snapshotId);
      var snapshot =
          IcebergBridge.mockSnapshot(
              snapshotId + 1,
              snapshotId + 1,
              snapshotId > 0 ? (long) snapshotId : null,
              System.currentTimeMillis(),
              "APPEND",
              Map.of(),
              schema.schemaId(),
              manifestList,
              (long) numManifestFiles * numManifestFiles);
      tableMetadataBuilder.addSnapshot(snapshot);
    }
    tableMetadata = tableMetadataBuilder.build();

    tableMetadataString = TableMetadataParser.toJson(tableMetadata);
    tableMetadataBytes = tableMetadataString.getBytes(UTF_8);
  }

  public String manifestListPath(int snapshotId) {
    return format("%s%05d/snap-%d.avro", prefix, snapshotId, snapshotId);
  }

  public byte[] serializedManifestList(long snapshotId) {
    var output = new ByteArrayOutputStream();
    OutputFile outputFile = new InMemoryOutputFile(output);

    try (FileAppender<ManifestFile> manifestListWriter =
        Avro.write(outputFile)
            .schema(ManifestFile.schema())
            .named("manifest_file")
            .overwrite()
            .build()) {
      for (int i = 0; i < numManifestFiles; i++) {
        var manifestPath = manifestFilePath(snapshotId, i);

        // Create a manifest file and get its ManifestFile metadata
        var tempManifestOutput = new ByteArrayOutputStream();
        OutputFile tempManifestOutputFile =
            new InMemoryOutputFile(tempManifestOutput, manifestPath);
        ManifestWriter<DataFile> tempWriter =
            org.apache.iceberg.ManifestFiles.write(2, spec, tempManifestOutputFile, snapshotId);
        for (int j = 0; j < numDataFiles; j++) {
          var dataFilePath = format("%s%05d/%05d/%05d/data.parquet", prefix, snapshotId, i, j);
          DataFile dataFile =
              DataFiles.builder(spec)
                  .withFormat(FileFormat.PARQUET)
                  .withPath(dataFilePath)
                  .withFileSizeInBytes(1024)
                  .withRecordCount(1)
                  .build();
          tempWriter.add(dataFile);
        }
        tempWriter.close();
        ManifestFile manifestFile = tempWriter.toManifestFile();

        manifestListWriter.add(manifestFile);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteArray();
  }

  public String manifestFilePath(long snapshotId, int file) {
    return format("%s%05d/%05d/xyz-m-manifest.avro", prefix, snapshotId, file);
  }

  public byte[] serializedManifestFile(long snapshotId, int manifest, String path) {
    var output = new ByteArrayOutputStream();
    OutputFile outputFile = new InMemoryOutputFile(output);

    try (ManifestWriter<DataFile> manifestFileWriter =
        org.apache.iceberg.ManifestFiles.write(2, spec, outputFile, snapshotId)) {
      for (int i = 0; i < numDataFiles; i++) {
        var dataFilePath = format("%s%05d/%05d/%05d/data.parquet", prefix, snapshotId, manifest, i);
        DataFile dataFile =
            DataFiles.builder(spec)
                .withFormat(FileFormat.PARQUET)
                .withPath(dataFilePath)
                .withFileSizeInBytes(1024)
                .withRecordCount(1)
                .build();
        manifestFileWriter.add(dataFile);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteArray();
  }

  private record InMemoryOutputFile(ByteArrayOutputStream output, String path)
      implements OutputFile {
    InMemoryOutputFile(ByteArrayOutputStream output) {
      this(output, "in-memory");
    }

    @Override
    public PositionOutputStream create() {
      return new ByteArrayPositionOutputStream(output);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      output.reset();
      return new ByteArrayPositionOutputStream(output);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  private static class ByteArrayPositionOutputStream
      extends org.apache.iceberg.io.PositionOutputStream {
    private final ByteArrayOutputStream output;

    ByteArrayPositionOutputStream(ByteArrayOutputStream output) {
      this.output = output;
    }

    @Override
    public long getPos() {
      return output.size();
    }

    @Override
    public void write(int b) throws IOException {
      output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      output.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      output.close();
    }
  }
}
