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
package org.apache.polaris.service.catalog.conversion.xtable;

import static org.apache.polaris.service.catalog.conversion.xtable.TableFormat.DELTA;
import static org.apache.polaris.service.catalog.conversion.xtable.TableFormat.ICEBERG;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.ENABLED_READ_TABLE_FORMATS_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.SOURCE_METADATA_PATH_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class XTableConverterTest {
  private static final String HOST_URL = "http://localhost";
  public static final int SUCCESS_STATUS_CODE = 200;
  private static final int SERVER_ERROR_STATUS_CODE = 500;
  private static final String DELTA_METADATA_PATH = "s3://bucket/some_table/delta_log";
  private static final String ICEBERG_METADATA_PATH = "s3://bucket/some_table/metadata";

  private static final String REQUEST_BODY_DELTA_SOURCE =
      "{\"source-format\":\"DELTA\", \"target-format\":\"ICEBERG\", \"source-metadata-path\":\""
          + DELTA_METADATA_PATH
          + "\"}";
  private static final String RESPONSE_BODY_ICEBERG_TARGET =
      "{\"target-metadata-path\":\"" + ICEBERG_METADATA_PATH + "\"}";
  private static final String REQUEST_BODY_ICEBERG_SOURCE =
      "{\"source-format\":\"ICEBERG\", \"target-format\":\"DELTA\", \"source-metadata-path\":\""
          + ICEBERG_METADATA_PATH
          + "\"}";
  private static final String RESPONSE_BODY_DELTA_TARGET =
      "{\"target-metadata-path\":\"" + DELTA_METADATA_PATH + "\"}";

  private HttpClient mockClient;
  private ObjectMapper mockMapper;
  private HttpResponse<String> mockResponse;
  private XTableConverter converter;

  @BeforeEach
  void setUp() throws Exception {
    mockClient = mock(HttpClient.class);
    mockMapper = mock(ObjectMapper.class);
    mockResponse = mock(HttpResponse.class);

    // inject mocks into private constructor
    Field inst = XTableConverter.class.getDeclaredField("INSTANCE");
    inst.setAccessible(true);
    inst.set(null, null);

    Constructor<XTableConverter> ctor =
        XTableConverter.class.getDeclaredConstructor(
            String.class, HttpClient.class, ObjectMapper.class);
    ctor.setAccessible(true);
    XTableConverter testInst = ctor.newInstance(HOST_URL, mockClient, mockMapper);
    inst.set(null, testInst);
    converter = testInst;
  }

  @Test
  void executeConversionOnGenericTable() throws Exception {
    GenericTableEntity generic = mock(GenericTableEntity.class);
    when(generic.getSubType()).thenReturn(PolarisEntitySubType.GENERIC_TABLE);
    when(generic.getFormat()).thenReturn(DELTA.name());
    Map<String, String> props = new HashMap<>();
    props.put(SOURCE_METADATA_PATH_KEY, DELTA_METADATA_PATH);
    props.put(ENABLED_READ_TABLE_FORMATS_KEY, ICEBERG.name());
    when(generic.getPropertiesAsMap()).thenReturn(props);

    ArgumentCaptor<RunSyncRequest> reqCap = ArgumentCaptor.forClass(RunSyncRequest.class);
    when(mockMapper.writeValueAsString(reqCap.capture())).thenReturn(REQUEST_BODY_DELTA_SOURCE);
    when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
    when(mockResponse.statusCode()).thenReturn(SUCCESS_STATUS_CODE);
    when(mockResponse.body()).thenReturn(RESPONSE_BODY_ICEBERG_TARGET);
    RunSyncResponse expected = new RunSyncResponse(ICEBERG.name(), ICEBERG_METADATA_PATH);
    when(mockMapper.readValue(RESPONSE_BODY_ICEBERG_TARGET, RunSyncResponse.class))
        .thenReturn(expected);

    RunSyncResponse actual = converter.execute(generic);

    assertSame(expected, actual);
    RunSyncRequest capturedReq = reqCap.getValue();
    assertEquals(DELTA.name(), capturedReq.getSourceFormat());
    assertEquals(DELTA_METADATA_PATH, capturedReq.getSourceMetadataPath());
    assertEquals(ICEBERG.name(), capturedReq.getTargetFormat());
  }

  @Test
  void executeConversionOnIcebergTable() throws Exception {
    IcebergTableLikeEntity iceberg = mock(IcebergTableLikeEntity.class);
    when(iceberg.getSubType()).thenReturn(PolarisEntitySubType.ICEBERG_TABLE);
    when(iceberg.getMetadataLocation()).thenReturn(ICEBERG_METADATA_PATH);
    Map<String, String> props = new HashMap<>();
    props.put(ENABLED_READ_TABLE_FORMATS_KEY, DELTA.name());
    when(iceberg.getPropertiesAsMap()).thenReturn(props);

    ArgumentCaptor<RunSyncRequest> reqCap = ArgumentCaptor.forClass(RunSyncRequest.class);
    when(mockMapper.writeValueAsString(reqCap.capture())).thenReturn(REQUEST_BODY_ICEBERG_SOURCE);
    when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
    when(mockResponse.statusCode()).thenReturn(SUCCESS_STATUS_CODE);
    when(mockResponse.body()).thenReturn(RESPONSE_BODY_DELTA_TARGET);
    RunSyncResponse expected = new RunSyncResponse(DELTA.name(), DELTA_METADATA_PATH);
    when(mockMapper.readValue(RESPONSE_BODY_DELTA_TARGET, RunSyncResponse.class))
        .thenReturn(expected);

    RunSyncResponse actual = converter.execute(iceberg);

    assertSame(expected, actual);
    RunSyncRequest capturedReq = reqCap.getValue();
    assertEquals(ICEBERG.name(), capturedReq.getSourceFormat());
    assertEquals(ICEBERG_METADATA_PATH, capturedReq.getSourceMetadataPath());
    assertEquals(DELTA.name(), capturedReq.getTargetFormat());
  }

  @Test
  void executeConversionFailure() throws Exception {
    GenericTableEntity genericTableEntity = mock(GenericTableEntity.class);
    when(genericTableEntity.getSubType()).thenReturn(PolarisEntitySubType.GENERIC_TABLE);
    when(genericTableEntity.getFormat()).thenReturn(DELTA.name());
    Map<String, String> props = new HashMap<>();
    props.put(SOURCE_METADATA_PATH_KEY, DELTA_METADATA_PATH);
    props.put(ENABLED_READ_TABLE_FORMATS_KEY, ICEBERG.name());
    when(genericTableEntity.getPropertiesAsMap()).thenReturn(props);

    when(mockMapper.writeValueAsString(any())).thenReturn(REQUEST_BODY_DELTA_SOURCE);
    when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
    when(mockResponse.statusCode()).thenReturn(SERVER_ERROR_STATUS_CODE);
    when(mockResponse.body()).thenReturn("Internal Service Exception");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> converter.execute(genericTableEntity));
    assertTrue(exception.getMessage().contains("Internal Service Exception"));
  }
}
