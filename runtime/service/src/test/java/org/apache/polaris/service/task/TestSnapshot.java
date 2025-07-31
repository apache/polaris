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
  private final long sequenceNumber;
  private final long snapshotId;
  private final long parentSnapshot;
  private final long timestampMillis;
  private final String manifestListLocation;

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
