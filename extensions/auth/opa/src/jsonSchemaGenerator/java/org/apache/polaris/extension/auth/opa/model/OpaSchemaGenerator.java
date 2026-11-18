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
package org.apache.polaris.extension.auth.opa.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility to generate JSON Schema from the OPA input model classes.
 *
 * <p>This can be run as a standalone utility to generate the JSON Schema document that can be
 * referenced in documentation and used by OPA policy developers.
 *
 * <p>Usage: java org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator [output-file-path]
 */
public class OpaSchemaGenerator {

  /**
   * Generates a JSON Schema for the OPA authorization input structure.
   *
   * @return the JSON Schema as a pretty-printed string
   * @throws IOException if schema generation fails
   */
  public static String generateSchema() throws IOException {
    return generateSchema(OpaAuthorizationInput.class);
  }

  /**
   * Generates a JSON Schema for the OPA batch authorization input structure.
   *
   * @return the JSON Schema as a pretty-printed string
   * @throws IOException if schema generation fails
   */
  public static String generateBatchSchema() throws IOException {
    return generateSchema(OpaBatchAuthorizationInput.class);
  }

  private static String generateSchema(Class<?> inputClass) throws IOException {
    ObjectMapper mapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

    JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
    JsonSchema schema = schemaGen.generateSchema(inputClass);

    return mapper.writeValueAsString(schema);
  }

  /**
   * Main method to generate and save the JSON Schemas to files.
   *
   * @param args optional output file paths: the input schema (defaults to opa-input-schema.json)
   *     and the batch input schema (defaults to opa-batch-input-schema.json)
   */
  public static void main(String[] args) throws IOException {
    write(generateSchema(), args.length > 0 ? args[0] : "opa-input-schema.json");
    write(generateBatchSchema(), args.length > 1 ? args[1] : "opa-batch-input-schema.json");
  }

  private static void write(String schemaJson, String path) throws IOException {
    Path outputPath = Paths.get(path);
    Files.writeString(outputPath, schemaJson);
    System.out.println("JSON Schema generated successfully at: " + outputPath.toAbsolutePath());
    System.out.println();
    System.out.println(schemaJson);
  }
}
