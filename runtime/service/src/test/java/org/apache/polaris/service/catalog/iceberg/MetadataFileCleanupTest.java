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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class MetadataFileCleanupTest {

  private static final String TABLE_LOCATION = "s3://bucket/ns/table";

  @Test
  void deletesRemovedMetadataFilesOnlyUnderTableLocations() throws IOException {
    InMemoryFileIO io = new InMemoryFileIO();
    String insideFile = TABLE_LOCATION + "/metadata/v1.metadata.json";
    String outsideFile = "s3://bucket/ns/other-table/metadata/v1.metadata.json";
    io.addFile(insideFile, new byte[] {1});
    io.addFile(outsideFile, new byte[] {1});

    TableMetadata newMetadata =
        TableMetadata.newTableMetadata(
            new Schema(required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            TABLE_LOCATION,
            Map.of(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true"));
    TableMetadata baseMetadata = withMetadataLog(newMetadata, insideFile, outsideFile);

    new MetadataFileCleanup(io, baseMetadata, newMetadata, Set.of(TABLE_LOCATION))
        .deleteRemovedMetadataFiles();

    assertThat(io.fileExists(insideFile)).isFalse();
    assertThat(io.fileExists(outsideFile)).isTrue();
  }

  private static TableMetadata withMetadataLog(TableMetadata metadata, String... files)
      throws IOException {
    ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(metadata));
    ArrayNode metadataLog = json.putArray("metadata-log");
    for (int i = 0; i < files.length; i++) {
      metadataLog.addObject().put("timestamp-ms", 1000L + i).put("metadata-file", files[i]);
    }
    return TableMetadataParser.fromJson(JsonUtil.mapper().writeValueAsString(json));
  }
}
