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
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
    JsonSchema schema = schemaGen.generateSchema(OpaAuthorizationInput.class);

    return mapper.writeValueAsString(schema);
  }

  /**
   * Main method to generate and save the JSON Schema to a file.
   *
   * @param args optional output file path (defaults to opa-input-schema.json)
   */
  public static void main(String[] args) throws IOException {
    String schemaJson = generateSchema();

    // Determine output path
    Path outputPath;
    if (args.length > 0) {
      outputPath = Paths.get(args[0]);
    } else {
      outputPath = Paths.get("opa-input-schema.json");
    }

    // Write schema to file
    Files.writeString(outputPath, schemaJson);
    System.out.println("JSON Schema generated successfully at: " + outputPath.toAbsolutePath());
    System.out.println();
    System.out.println(schemaJson);
  }
}
