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
package org.apache.polaris.service.persistence;

import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.polaris.service.catalog.iceberg.IcebergMetadataUtil;

/**
 * Represents a metadata.json file with its location and content
 *
 * @param location the location of the metadata.json itself
 * @param content the content of the metadata.json
 * @param tableLocations The locations that the table this metadata.json describes might be written.
 *     This includes the table base location as well as any data or metadata locations specified in
 *     the table properties
 */
public record MetadataJson(String location, String content, Set<String> tableLocations) {

  /** Construct a record from a {@link TableMetadata} object */
  public static MetadataJson fromMetadata(TableMetadata metadata) {
    Set<String> tableLocations = IcebergMetadataUtil.getLocationsAllowedToBeAccessed(metadata);
    return new MetadataJson(
        metadata.metadataFileLocation(), TableMetadataParser.toJson(metadata), tableLocations);
  }
}
