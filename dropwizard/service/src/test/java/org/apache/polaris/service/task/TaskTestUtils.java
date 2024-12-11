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
package org.apache.polaris.service.task;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.types.Types;

public class TaskTestUtils {
  static ManifestFile manifestFile(
      FileIO fileIO, String manifestFilePath, long snapshotId, String... dataFiles)
      throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            2, PartitionSpec.unpartitioned(), fileIO.newOutputFile(manifestFilePath), snapshotId);
    for (String dataFile : dataFiles) {
      writer.add(
          new DataFiles.Builder(PartitionSpec.unpartitioned())
              .withFileSizeInBytes(100L)
              .withFormat(FileFormat.PARQUET)
              .withPath(dataFile)
              .withRecordCount(10)
              .build());
    }
    writer.close();
    return writer.toManifestFile();
  }

  static TableMetadata writeTableMetadata(FileIO fileIO, String metadataFile, Snapshot... snapshots)
      throws IOException {
    return writeTableMetadata(fileIO, metadataFile, null, null, null, snapshots);
  }

  static TableMetadata writeTableMetadata(
      FileIO fileIO,
      String metadataFile,
      List<StatisticsFile> statisticsFiles,
      Snapshot... snapshots)
      throws IOException {
    return writeTableMetadata(fileIO, metadataFile, null, null, statisticsFiles, snapshots);
  }

  static TableMetadata writeTableMetadata(
      FileIO fileIO,
      String metadataFile,
      TableMetadata prevMetadata,
      String prevMetadataFile,
      List<StatisticsFile> statisticsFiles,
      Snapshot... snapshots)
      throws IOException {
    TableMetadata.Builder tmBuilder;
    if (prevMetadata == null) {
      tmBuilder = TableMetadata.buildFromEmpty();
    } else {
      tmBuilder = TableMetadata.buildFrom(prevMetadata).setPreviousFileLocation(prevMetadataFile);
    }
    tmBuilder
        .setLocation("path/to/table")
        .addSchema(
            new Schema(List.of(Types.NestedField.of(1, false, "field1", Types.StringType.get()))),
            1)
        .addSortOrder(SortOrder.unsorted())
        .assignUUID(UUID.randomUUID().toString())
        .addPartitionSpec(PartitionSpec.unpartitioned());

    int statisticsFileIndex = 0;
    for (Snapshot snapshot : snapshots) {
      tmBuilder.addSnapshot(snapshot);
      if (statisticsFiles != null) {
        tmBuilder.setStatistics(snapshot.snapshotId(), statisticsFiles.get(statisticsFileIndex++));
      }
    }
    TableMetadata tableMetadata = tmBuilder.build();
    PositionOutputStream out = fileIO.newOutputFile(metadataFile).createOrOverwrite();
    out.write(TableMetadataParser.toJson(tableMetadata).getBytes(StandardCharsets.UTF_8));
    out.close();
    return tableMetadata;
  }

  static @Nonnull TestSnapshot newSnapshot(
      FileIO fileIO,
      String manifestListLocation,
      long sequenceNumber,
      long snapshotId,
      long parentSnapshot,
      ManifestFile... manifestFiles)
      throws IOException {
    FileAppender<ManifestFile> manifestListWriter =
        Avro.write(fileIO.newOutputFile(manifestListLocation))
            .schema(ManifestFile.schema())
            .named("manifest_file")
            .overwrite()
            .build();
    manifestListWriter.addAll(Arrays.asList(manifestFiles));
    manifestListWriter.close();
    TestSnapshot snapshot =
        new TestSnapshot(sequenceNumber, snapshotId, parentSnapshot, 1L, manifestListLocation);
    return snapshot;
  }

  public static StatisticsFile writeStatsFile(
      long snapshotId, long snapshotSequenceNumber, String statsLocation, FileIO fileIO)
      throws IOException {
    try (PuffinWriter puffinWriter = Puffin.write(fileIO.newOutputFile(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              List.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();

      return new GenericStatisticsFile(
          snapshotId,
          statsLocation,
          puffinWriter.fileSize(),
          puffinWriter.footerSize(),
          puffinWriter.writtenBlobsMetadata().stream().map(GenericBlobMetadata::from).toList());
    }
  }
}
