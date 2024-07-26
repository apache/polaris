package io.polaris.service.task;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

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

  static void writeTableMetadata(FileIO fileIO, String metadataFile, Snapshot... snapshots)
      throws IOException {
    TableMetadata.Builder tmBuidler =
        TableMetadata.buildFromEmpty()
            .setLocation("path/to/table")
            .addSchema(
                new Schema(
                    List.of(Types.NestedField.of(1, false, "field1", Types.StringType.get()))),
                1)
            .addSortOrder(SortOrder.unsorted())
            .assignUUID(UUID.randomUUID().toString())
            .addPartitionSpec(PartitionSpec.unpartitioned());
    for (Snapshot snapshot : snapshots) {
      tmBuidler.addSnapshot(snapshot);
    }
    TableMetadata tableMetadata = tmBuidler.build();
    PositionOutputStream out = fileIO.newOutputFile(metadataFile).createOrOverwrite();
    out.write(TableMetadataParser.toJson(tableMetadata).getBytes(StandardCharsets.UTF_8));
    out.close();
  }

  static @NotNull TestSnapshot newSnapshot(
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
}
