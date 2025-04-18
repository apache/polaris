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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.junit.jupiter.api.Test;

public class StreamingLoadTableResponseTest {
  @Test
  public void testSerialization() throws IOException {
    TableMetadata metadata =
        TableMetadata.buildFromEmpty()
            .setLocation("s3://foo")
            .setCurrentSchema(
                new Schema(
                    0, List.of(Types.NestedField.required(1, "col1", Types.StringType.get()))),
                1)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .setDefaultSortOrder(SortOrder.unsorted())
            .build();
    IcebergCatalogHandler.StreamingLoadTableResponse resp =
        new IcebergCatalogHandler.StreamingLoadTableResponse(
            "s3://foo",
            new ByteArrayInputStream(
                TableMetadataParser.toJson(metadata).getBytes(Charset.defaultCharset())),
            Map.of(),
            List.of());
    ObjectMapper objectMapper = new ObjectMapper();
    RESTSerializers.registerAll(objectMapper);
    LoadTableResponse deserializedResponse =
        objectMapper.readValue(objectMapper.writeValueAsBytes(resp), LoadTableResponse.class);
    assertThat(deserializedResponse.metadataLocation()).isEqualTo("s3://foo");
    assertThat(deserializedResponse.tableMetadata())
        .returns("s3://foo", TableMetadata::location)
        .returns(0, TableMetadata::currentSchemaId)
        .extracting(TableMetadata::schema)
        .returns(
            List.of(Types.NestedField.required(1, "col1", Types.StringType.get())),
            Schema::columns);
  }
}
