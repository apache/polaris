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
package org.apache.polaris.service.catalog.semanticmodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.Map;

/** Loads and caches bundled Apache Ossie JSON Schemas for offline validation. */
final class OssieJsonSchemas {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonSchemaFactory SCHEMA_FACTORY =
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
  private static final SchemaValidatorsConfig SCHEMA_CONFIG =
      SchemaValidatorsConfig.builder().pathType(PathType.JSON_POINTER).build();
  private static final Map<OssieSpecVersion, JsonSchema> SCHEMAS = loadAll();

  private OssieJsonSchemas() {}

  static JsonSchema forVersion(OssieSpecVersion version) {
    JsonSchema schema = SCHEMAS.get(version);
    if (schema == null) {
      throw new IllegalStateException("Missing bundled JSON Schema for Ossie version " + version);
    }
    return schema;
  }

  private static Map<OssieSpecVersion, JsonSchema> loadAll() {
    Map<OssieSpecVersion, JsonSchema> loaded = new EnumMap<>(OssieSpecVersion.class);
    for (OssieSpecVersion version : OssieSpecVersion.values()) {
      loaded.put(version, loadSchema(version));
    }
    return loaded;
  }

  private static JsonSchema loadSchema(OssieSpecVersion version) {
    String resourcePath = version.schemaResourcePath();
    try (InputStream in =
        OssieJsonSchemas.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IllegalStateException(
            "Bundled Ossie JSON Schema not found on classpath: " + resourcePath);
      }
      JsonNode schemaNode = MAPPER.readTree(in);
      return SCHEMA_FACTORY.getSchema(schemaNode, SCHEMA_CONFIG);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read bundled Ossie JSON Schema: " + resourcePath, e);
    }
  }
}
