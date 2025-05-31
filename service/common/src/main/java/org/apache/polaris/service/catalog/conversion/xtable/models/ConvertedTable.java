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
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ConvertedTable {
  @JsonProperty("target-format")
  private String targetFormat;

  @JsonProperty("target-metadata-path")
  private String targetMetadataPath;

  @JsonProperty("target-schema")
  private String targetSchema;

  @JsonCreator
  public ConvertedTable(
      @JsonProperty String targetFormat,
      @JsonProperty String targetMetadataPath,
      @JsonProperty String targetSchema) {
    this.targetFormat = targetFormat;
    this.targetMetadataPath = targetMetadataPath;
    this.targetSchema = targetSchema;
  }
}
