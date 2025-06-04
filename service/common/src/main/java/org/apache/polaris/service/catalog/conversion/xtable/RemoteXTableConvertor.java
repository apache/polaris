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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RemoteXTableConvertor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteXTableConvertor.class);

  private static final int HTTP_SUCCESS_START = 200;
  private static final int HTTP_SUCCESS_END = 299;
  private static final String RUN_SYNC_ENDPOINT = "/v1/conversion/table";

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
      LOGGER.warn("RemoteXTableConvertor already initialized");
      return;
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
    Map<String, String> configurations = new HashMap<>();

    switch (tableEntity) {
      case GenericTableEntity genericTable -> {
        sourceFormat = checkIfSupportedFormat(genericTable.getFormat());
        sourceTableName = tableEntity.getName();
        sourceTablePath = genericTable.getPropertiesAsMap().get("location");
        sourceDataPath =
            genericTable.getPropertiesAsMap().getOrDefault(SOURCE_DATA_PATH_KEY, sourceTablePath);
        // TODO setting this for now since there is a bug
        // with polaris spark client: https://github.com/apache/polaris/issues/1785
        targetFormats =
            Arrays.asList(
                checkIfSupportedFormat(
                    genericTable
                        .getPropertiesAsMap()
                        .getOrDefault(ENABLED_READ_TABLE_FORMATS_KEY, "ICEBERG")));
      }
      case IcebergTableLikeEntity icebergTable -> {
        sourceFormat = TableFormat.ICEBERG.name();
        sourceTableName = tableEntity.getName();
        sourceTablePath = icebergTable.getMetadataLocation();
        sourceDataPath = icebergTable.getBaseLocation() + "/data";
        // TODO setting this for now as some issue with table properties in polaris client
        // once fixed we can get from table prop the value ("HUDI", "DELTA")
        targetFormats =
            Arrays.asList(
                icebergTable
                    .getPropertiesAsMap()
                    .getOrDefault(ENABLED_READ_TABLE_FORMATS_KEY, "DELTA"));
      }
      default -> throw new IllegalArgumentException("Unsupported TableEntity: " + tableEntity);
    }

    return executeRunSyncRequest(
        sourceFormat,
        sourceTableName,
        sourceTablePath,
        sourceDataPath,
        targetFormats,
        configurations);
  }

  private ConvertTableResponse executeRunSyncRequest(
      String sourceFormat,
      String sourceTableName,
      String sourceTablePath,
      String sourceDataPath,
      List<String> targetFormats,
      Map<String, String> configurations) {

    ConvertTableRequest request =
        new ConvertTableRequest(
            sourceFormat,
            sourceTableName,
            sourceTablePath,
            sourceDataPath,
            targetFormats,
            configurations);
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
