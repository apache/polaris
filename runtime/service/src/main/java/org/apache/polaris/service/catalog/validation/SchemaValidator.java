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
package org.apache.polaris.service.catalog.validation;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Validates Iceberg schemas for case-insensitive catalog requirements.
 *
 * <p>In case-insensitive catalogs, column names are NOT normalized (preserving original case), but
 * we enforce that no two columns differ only by case to avoid ambiguity during queries.
 */
@ApplicationScoped
public class SchemaValidator {

  /**
   * Validate that no two column names in the schema differ only by case.
   *
   * @param schema the Iceberg schema to validate
   * @throws BadRequestException if case-conflicting column names are found
   */
  public void validateNoCaseConflicts(Schema schema) {
    if (schema == null) {
      return;
    }

    Set<String> seenLowercase = new HashSet<>();
    for (Types.NestedField field : schema.columns()) {
      validateFieldNoCaseConflicts(field, seenLowercase, "");
    }
  }

  private void validateFieldNoCaseConflicts(
      Types.NestedField field, Set<String> seenLowercase, String parentPath) {

    String fieldPath = parentPath.isEmpty() ? field.name() : parentPath + "." + field.name();
    String lowercasePath = fieldPath.toLowerCase(Locale.ROOT);

    if (!seenLowercase.add(lowercasePath)) {
      throw new BadRequestException(
          "Schema contains case-conflicting column names: '%s'. "
              + "In case-insensitive catalogs, column names must be unique regardless of case.",
          fieldPath);
    }

    // Recursively validate nested types
    Type type = field.type();
    if (type.isStructType()) {
      Types.StructType structType = type.asStructType();
      for (Types.NestedField nestedField : structType.fields()) {
        validateFieldNoCaseConflicts(nestedField, seenLowercase, fieldPath);
      }
    } else if (type.isListType()) {
      Types.ListType listType = type.asListType();
      if (listType.elementType().isStructType()) {
        for (Types.NestedField nestedField : listType.elementType().asStructType().fields()) {
          validateFieldNoCaseConflicts(nestedField, seenLowercase, fieldPath + ".element");
        }
      }
    } else if (type.isMapType()) {
      Types.MapType mapType = type.asMapType();
      // Validate key struct if present
      if (mapType.keyType().isStructType()) {
        for (Types.NestedField nestedField : mapType.keyType().asStructType().fields()) {
          validateFieldNoCaseConflicts(nestedField, seenLowercase, fieldPath + ".key");
        }
      }
      // Validate value struct if present
      if (mapType.valueType().isStructType()) {
        for (Types.NestedField nestedField : mapType.valueType().asStructType().fields()) {
          validateFieldNoCaseConflicts(nestedField, seenLowercase, fieldPath + ".value");
        }
      }
    }
  }
}
