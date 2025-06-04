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

package org.apache.polaris.service.catalog.conversion.xtable.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ConvertTableRequest {
  @JsonProperty("source-format")
  private String sourceFormat;

  @JsonProperty("source-table-name")
  private String sourceTableName;

  @JsonProperty("source-table-path")
  private String sourceTablePath;

  @JsonProperty("source-data-path")
  private String sourceDataPath;

  @JsonProperty("target-formats")
  private List<String> targetFormats;

  @JsonProperty("configurations")
  private Map<String, String> configurations;

  public ConvertTableRequest() {}

  @JsonCreator
  public ConvertTableRequest(
      @JsonProperty("source-format") String sourceFormat,
      @JsonProperty("source-table-name") String sourceTableName,
      @JsonProperty("source-table-path") String sourceTablePath,
      @JsonProperty("source-data-path") String sourceDataPath,
      @JsonProperty("target-format") List<String> targetFormat,
      @JsonProperty("configurations") Map<String, String> configurations) {

    this.sourceFormat = sourceFormat;
    this.sourceTableName = sourceTableName;
    this.sourceTablePath = sourceTablePath;
    this.sourceDataPath = sourceDataPath;
    this.targetFormats = targetFormat;
    this.configurations = configurations;
  }
}
