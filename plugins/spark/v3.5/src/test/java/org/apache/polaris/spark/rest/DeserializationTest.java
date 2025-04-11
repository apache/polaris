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
package org.apache.polaris.spark.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.service.types.GenericTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeserializationTest {
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
  }

  @ParameterizedTest
  @MethodSource("genericTableTestCases")
  public void testLoadGenericTableRESTResponse(String doc, Map<String, String> properties)
      throws JsonProcessingException {
    GenericTable table =
        GenericTable.builder()
            .setFormat("delta")
            .setName("test-table")
            .setProperties(properties)
            .setDoc(doc)
            .build();
    LoadGenericTableRESTResponse response = new LoadGenericTableRESTResponse(table);
    String json = mapper.writeValueAsString(response);
    LoadGenericTableRESTResponse deserializedResponse =
        mapper.readValue(json, LoadGenericTableRESTResponse.class);
    assertThat(deserializedResponse.getTable().getFormat()).isEqualTo("delta");
    assertThat(deserializedResponse.getTable().getName()).isEqualTo("test-table");
    assertThat(deserializedResponse.getTable().getDoc()).isEqualTo(doc);
    assertThat(deserializedResponse.getTable().getProperties().size()).isEqualTo(properties.size());
  }

  @ParameterizedTest
  @MethodSource("genericTableTestCases")
  public void testCreateGenericTableRESTRequest(String doc, Map<String, String> properties)
      throws JsonProcessingException {
    CreateGenericTableRESTRequest request =
        new CreateGenericTableRESTRequest("test-table", "delta", doc, properties);
    String json = mapper.writeValueAsString(request);
    CreateGenericTableRESTRequest deserializedRequest =
        mapper.readValue(json, CreateGenericTableRESTRequest.class);
    assertThat(deserializedRequest.getName()).isEqualTo("test-table");
    assertThat(deserializedRequest.getFormat()).isEqualTo("delta");
    assertThat(deserializedRequest.getDoc()).isEqualTo(doc);
    assertThat(deserializedRequest.getProperties().size()).isEqualTo(properties.size());
  }

  private static Stream<Arguments> genericTableTestCases() {
    var doc = "table for testing";
    var properties = Maps.newHashMap();
    properties.put("location", "s3://path/to/table/");
    return Stream.of(
        Arguments.of(doc, properties),
        Arguments.of(null, Maps.newHashMap()),
        Arguments.of(doc, Maps.newHashMap()),
        Arguments.of(null, properties));
  }
}
