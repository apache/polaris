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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonFactory;
import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonFactoryBuilder;
import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeserializationTest {
  private ObjectMapper mapper;
  private static final JsonFactory FACTORY =
      new JsonFactoryBuilder()
          .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
          .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
          .build();

  @BeforeEach
  public void setUp() {
    // NOTE: This is the same setting as iceberg RESTObjectMapper.java. However,
    // RESTObjectMapper is not a public class, therefore, we duplicate the
    // setting here for serialization and deserialization tests.
    mapper = new ObjectMapper(FACTORY);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
  }

  @ParameterizedTest
  @MethodSource("genericTableTestCases")
  public void testLoadGenericTableRESTResponse(
      String baseLocation, String doc, Map<String, String> properties)
      throws JsonProcessingException {
    GenericTable.Builder tableBuilder =
        GenericTable.builder()
            .setFormat("delta")
            .setName("test-table")
            .setProperties(properties)
            .setDoc(doc);
    if (baseLocation != null) {
      tableBuilder.setBaseLocation(baseLocation);
    }
    GenericTable table = tableBuilder.build();
    LoadGenericTableRESTResponse response = new LoadGenericTableRESTResponse(table);
    String json = mapper.writeValueAsString(response);
    LoadGenericTableRESTResponse deserializedResponse =
        mapper.readValue(json, LoadGenericTableRESTResponse.class);
    assertThat(deserializedResponse.getTable().getFormat()).isEqualTo("delta");
    assertThat(deserializedResponse.getTable().getName()).isEqualTo("test-table");
    assertThat(deserializedResponse.getTable().getDoc()).isEqualTo(doc);
    assertThat(deserializedResponse.getTable().getProperties().size()).isEqualTo(properties.size());
    assertThat(deserializedResponse.getTable().getBaseLocation()).isEqualTo(baseLocation);
  }

  @ParameterizedTest
  @MethodSource("genericTableTestCases")
  public void testCreateGenericTableRESTRequest(
      String baseLocation, String doc, Map<String, String> properties)
      throws JsonProcessingException {
    CreateGenericTableRESTRequest request =
        new CreateGenericTableRESTRequest(
            CreateGenericTableRequest.builder()
                .setName("test-table")
                .setFormat("delta")
                .setDoc(doc)
                .setBaseLocation(baseLocation)
                .setProperties(properties)
                .build());
    String json = mapper.writeValueAsString(request);
    CreateGenericTableRESTRequest deserializedRequest =
        mapper.readValue(json, CreateGenericTableRESTRequest.class);
    assertThat(deserializedRequest.getName()).isEqualTo("test-table");
    assertThat(deserializedRequest.getFormat()).isEqualTo("delta");
    assertThat(deserializedRequest.getDoc()).isEqualTo(doc);
    assertThat(deserializedRequest.getProperties().size()).isEqualTo(properties.size());
    assertThat(deserializedRequest.getBaseLocation()).isEqualTo(baseLocation);
  }

  @Test
  public void testListGenericTablesRESTResponse() throws JsonProcessingException {
    Namespace namespace = Namespace.of("test-ns");
    Set<TableIdentifier> idents =
        ImmutableSet.of(
            TableIdentifier.of(namespace, "table1"),
            TableIdentifier.of(namespace, "table2"),
            TableIdentifier.of(namespace, "table3"));

    // page token is null
    ListGenericTablesRESTResponse response = new ListGenericTablesRESTResponse(null, idents);
    String json = mapper.writeValueAsString(response);
    ListGenericTablesRESTResponse deserializedResponse =
        mapper.readValue(json, ListGenericTablesRESTResponse.class);
    assertThat(deserializedResponse.getNextPageToken()).isNull();
    assertThat(deserializedResponse.getIdentifiers().size()).isEqualTo(idents.size());
    for (TableIdentifier identifier : idents) {
      assertThat(deserializedResponse.getIdentifiers()).contains(identifier);
    }

    // page token is not null
    response = new ListGenericTablesRESTResponse("page-token", idents);
    json = mapper.writeValueAsString(response);
    deserializedResponse = mapper.readValue(json, ListGenericTablesRESTResponse.class);
    assertThat(deserializedResponse.getNextPageToken()).isEqualTo("page-token");
    for (TableIdentifier identifier : idents) {
      assertThat(deserializedResponse.getIdentifiers()).contains(identifier);
    }
  }

  @Test
  public void testLoadGenericTableRestResponse() throws JsonProcessingException {
    LoadGenericTableRESTResponse request =
        new LoadGenericTableRESTResponse(
            GenericTable.builder().setName("test-table").setFormat("delta").build());
    String json = mapper.writeValueAsString(request);
    LoadGenericTableRESTResponse deserializedResponse =
        mapper.readValue(json, LoadGenericTableRESTResponse.class);
    assertThat(deserializedResponse.getTable().getName()).isEqualTo("test-table");
  }

  private static Stream<Arguments> genericTableTestCases() {
    var doc = "table for testing";
    var properties = Maps.newHashMap();
    properties.put("location", "s3://path/to/table/");
    var baseLocation = "s3://path/to/table/";
    return Stream.of(
        Arguments.of(null, doc, properties),
        Arguments.of(baseLocation, doc, properties),
        Arguments.of(null, null, Maps.newHashMap()),
        Arguments.of(baseLocation, doc, Maps.newHashMap()),
        Arguments.of(baseLocation, null, properties));
  }
}
