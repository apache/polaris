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

import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.ENABLED_READ_TABLE_FORMATS_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.SOURCE_DATA_PATH_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.SOURCE_TABLE_PATH_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.TableLikeEntity;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertTableRequest;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertTableResponse;

public final class RemoteXTableConvertor {
  private static final int HTTP_SUCCESS_START = 200;
  private static final int HTTP_SUCCESS_END = 299;
  private static final String RUN_SYNC_ENDPOINT = "/v1/conversion/sync";

  private static RemoteXTableConvertor INSTANCE;
  private final String hostUrl;
  private final HttpClient client;
  private final ObjectMapper mapper;

  private RemoteXTableConvertor(String hostUrl, HttpClient client, ObjectMapper mapper) {
    if (hostUrl == null || hostUrl.isBlank()) {
      throw new IllegalArgumentException("hostUrl must be provided");
    }
    this.hostUrl = hostUrl;
    this.client = client;
    this.mapper = mapper;
  }

  public static void initialize(String hostUrl) {
    if (INSTANCE != null) {
      throw new IllegalStateException("RemoteXTableConvertor already initialized");
    }
    INSTANCE =
        new RemoteXTableConvertor(
            hostUrl,
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build(),
            new ObjectMapper());
  }

  public static RemoteXTableConvertor getInstance() {
    return INSTANCE;
  }

  public ConvertTableResponse execute(TableLikeEntity tableEntity) {
    String sourceFormat;
    String sourceTableName;
    String sourceTablePath;
    String sourceDataPath;
    List<String> targetFormats;
    Map<String, String> configurations;

    switch (tableEntity.getSubType()) {
      case GENERIC_TABLE -> {
        GenericTableEntity generic = (GenericTableEntity) tableEntity;
        sourceFormat = checkIfSupportedFormat(generic.getFormat());
        sourceTableName = tableEntity.getName();
        sourceTablePath = generic.getPropertiesAsMap().get(SOURCE_TABLE_PATH_KEY);
        sourceDataPath = generic.getPropertiesAsMap().get(SOURCE_DATA_PATH_KEY);
        targetFormats = Arrays.asList(checkIfSupportedFormat(
                generic.getPropertiesAsMap().get(ENABLED_READ_TABLE_FORMATS_KEY)));
      }
      case ICEBERG_TABLE -> {
        IcebergTableLikeEntity iceberg = (IcebergTableLikeEntity) tableEntity;
        sourceFormat = TableFormat.ICEBERG.name();
        sourceTableName = tableEntity.getName();
        sourceTablePath = iceberg.getBaseLocation();
        sourceDataPath = iceberg.getBaseLocation() + "/data";
        targetFormats = Arrays.asList(iceberg.getPropertiesAsMap().get(ENABLED_READ_TABLE_FORMATS_KEY));
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported TableEntity type: " + tableEntity.getSubType());
    }

    return executeRunSyncRequest(
        sourceFormat,
        sourceTableName,
        sourceTablePath,
        sourceDataPath,
        targetFormats,
        new HashMap<>());
  }

  private ConvertTableResponse executeRunSyncRequest(
          String sourceFormat, String sourceTableName, String sourceTablePath,
          String sourceDataPath, List<String> targetFormats, Map<String, String> configurations) {

    ConvertTableRequest request = new ConvertTableRequest(sourceFormat, sourceTableName, sourceTablePath, sourceDataPath, targetFormats, configurations);
    try {
      String requestBody = mapper.writeValueAsString(request);
      HttpRequest httpRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(hostUrl + RUN_SYNC_ENDPOINT))
              .header("Accept", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      HttpResponse<String> response =
          client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
      if (!isSuccessStatus(response.statusCode())) {
        throw new IllegalStateException("Conversion failed: " + response.body());
      }
      return mapper.readValue(response.body(), ConvertTableResponse.class);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static String checkIfSupportedFormat(String format) {
    TableFormat tableFormat = TableFormat.fromName(format);
    return tableFormat.name();
  }

  public static boolean isSuccessStatus(int statusCode) {
    return statusCode >= HTTP_SUCCESS_START && statusCode <= HTTP_SUCCESS_END;
  }
}
