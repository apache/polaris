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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.IcebergBridge;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriterSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriterSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;

public class IcebergFixtures {
  public final Schema schema;
  public final IcebergSchema nessieIcebergSchema;
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
    try {
      nessieIcebergSchema =
          new ObjectMapper().readValue(SchemaParser.toJson(schema), IcebergSchema.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
    try (var manifestListWriter =
        IcebergManifestListWriter.openManifestListWriter(
            IcebergManifestListWriterSpec.builder()
                .snapshotId(snapshotId)
                .sequenceNumber(snapshotId)
                .parentSnapshotId(snapshotId > 0 ? snapshotId - 1 : null)
                .partitionSpec(IcebergPartitionSpec.UNPARTITIONED_SPEC)
                .spec(IcebergSpec.V2)
                .schema(nessieIcebergSchema)
                .build(),
            output)) {
      for (int i = 0; i < numManifestFiles; i++) {
        var manifestPath = manifestFilePath(snapshotId, i);
        var manifestFile =
            IcebergManifestFile.builder()
                .addedFilesCount(numDataFiles)
                .addedRowsCount((long) numDataFiles)
                .existingFilesCount(0)
                .existingRowsCount(0L)
                .deletedFilesCount(0)
                .deletedRowsCount(0L)
                .content(IcebergManifestContent.DATA)
                .manifestLength(1024)
                .minSequenceNumber(snapshotId)
                .sequenceNumber(snapshotId)
                .manifestPath(manifestPath)
                .partitionSpecId(IcebergPartitionSpec.UNPARTITIONED_SPEC.specId())
                .addedSnapshotId(snapshotId)
                .build();
        manifestListWriter.append(manifestFile);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return output.toByteArray();
  }

  public String manifestFilePath(long snapshotId, int file) {
    return format("%s%05d/%05d/xyz-m-manifest.avro", prefix, snapshotId, file);
  }

  public byte[] serializedManifestFile(long snapshotId, int manifest, String path) {
    var output = new ByteArrayOutputStream();
    var spec = IcebergPartitionSpec.UNPARTITIONED_SPEC;
    var partition = new GenericData.Record(spec.avroSchema(nessieIcebergSchema, "r102"));
    try (var manifestFileWriter =
        IcebergManifestFileWriter.openManifestFileWriter(
            IcebergManifestFileWriterSpec.builder()
                .addedSnapshotId(snapshotId)
                .manifestPath(path)
                .content(IcebergManifestContent.DATA)
                .minSequenceNumber(snapshotId)
                .sequenceNumber(snapshotId)
                .partitionSpec(spec)
                .schema(nessieIcebergSchema)
                .spec(IcebergSpec.V2)
                .build(),
            output)) {
      for (int i = 0; i < numDataFiles; i++) {
        var manifestPath = format("%s%05d/%05d/%05d/data.parquet", prefix, snapshotId, manifest, i);
        manifestFileWriter.append(
            IcebergManifestEntry.builder()
                .dataFile(
                    IcebergDataFile.builder()
                        .fileFormat(IcebergFileFormat.PARQUET)
                        .filePath(manifestPath)
                        .specId(spec.specId())
                        .content(IcebergDataContent.DATA)
                        .fileSizeInBytes(1024)
                        .partition(partition)
                        .recordCount(1)
                        .build())
                .sequenceNumber(snapshotId)
                .fileSequenceNumber(snapshotId)
                .snapshotId(snapshotId)
                .status(IcebergManifestEntryStatus.ADDED)
                .build());
      }
      manifestFileWriter.finish();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return output.toByteArray();
  }
}
