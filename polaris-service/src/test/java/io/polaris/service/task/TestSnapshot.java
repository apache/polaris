package io.polaris.service.task;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

final class TestSnapshot implements Snapshot {
  private long sequenceNumber;
  private long snapshotId;
  private long parentSnapshot;
  private long timestampMillis;
  private String manifestListLocation;

  public TestSnapshot(
      long sequenceNumber,
      long snapshotId,
      long parentSnapshot,
      long timestampMillis,
      String manifestListLocation) {
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentSnapshot = parentSnapshot;
    this.timestampMillis = timestampMillis;
    this.manifestListLocation = manifestListLocation;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentSnapshot;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public List<ManifestFile> allManifests(FileIO io) {
    try (CloseableIterable<ManifestFile> files =
        Avro.read(io.newInputFile(manifestListLocation))
            .rename("manifest_file", GenericManifestFile.class.getName())
            .rename("partitions", GenericPartitionFieldSummary.class.getName())
            .rename("r508", GenericPartitionFieldSummary.class.getName())
            .classLoader(GenericManifestFile.class.getClassLoader())
            .project(ManifestFile.schema())
            .reuseContainers(false)
            .build()) {

      return Lists.newLinkedList(files);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Cannot read manifest list file: %s", manifestListLocation);
    }
  }

  @Override
  public List<ManifestFile> dataManifests(FileIO io) {
    return allManifests(io).stream()
        .filter(mf -> mf.content().equals(ManifestContent.DATA))
        .toList();
  }

  @Override
  public List<ManifestFile> deleteManifests(FileIO io) {
    return allManifests(io).stream()
        .filter(mf -> mf.content().equals(ManifestContent.DELETES))
        .toList();
  }

  @Override
  public String operation() {
    return "op";
  }

  @Override
  public Map<String, String> summary() {
    return Map.of();
  }

  @Override
  public Iterable<DataFile> addedDataFiles(FileIO io) {
    return null;
  }

  @Override
  public Iterable<DataFile> removedDataFiles(FileIO io) {
    return null;
  }

  @Override
  public String manifestListLocation() {
    return manifestListLocation;
  }
}
