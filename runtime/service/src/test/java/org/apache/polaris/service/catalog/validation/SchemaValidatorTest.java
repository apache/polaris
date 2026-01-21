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

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaValidatorTest {

  private final SchemaValidator validator = new SchemaValidator();

  @Test
  public void testValidateNullSchema() {
    // Should not throw
    validator.validateNoCaseConflicts(null);
  }

  @Test
  public void testValidateEmptySchema() {
    Schema schema = new Schema();
    // Should not throw
    validator.validateNoCaseConflicts(schema);
  }

  @Test
  public void testValidateSchemaWithUniqueColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()));

    // Should not throw
    validator.validateNoCaseConflicts(schema);
  }

  @Test
  public void testValidateSchemaWithCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "ID", Types.IntegerType.get()));

    Assertions.assertThatThrownBy(() -> validator.validateNoCaseConflicts(schema))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("case-conflicting column names")
        .hasMessageContaining("ID");
  }

  @Test
  public void testValidateSchemaWithMixedCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "myColumn", Types.IntegerType.get()),
            Types.NestedField.required(2, "MyColumn", Types.IntegerType.get()));

    Assertions.assertThatThrownBy(() -> validator.validateNoCaseConflicts(schema))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("case-conflicting column names");
  }

  @Test
  public void testValidateSchemaWithNestedStructNoCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(3, "inner_id", Types.IntegerType.get()),
                    Types.NestedField.required(4, "inner_name", Types.StringType.get()))));

    // Should not throw
    validator.validateNoCaseConflicts(schema);
  }

  @Test
  public void testValidateSchemaWithNestedStructCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(3, "value", Types.IntegerType.get()),
                    Types.NestedField.required(4, "VALUE", Types.IntegerType.get()))));

    Assertions.assertThatThrownBy(() -> validator.validateNoCaseConflicts(schema))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("case-conflicting column names")
        .hasMessageContaining("nested.VALUE");
  }

  @Test
  public void testValidateSchemaWithListOfStructsNoCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "items",
                Types.ListType.ofRequired(
                    3,
                    Types.StructType.of(
                        Types.NestedField.required(4, "name", Types.StringType.get()),
                        Types.NestedField.required(5, "count", Types.IntegerType.get())))));

    // Should not throw
    validator.validateNoCaseConflicts(schema);
  }

  @Test
  public void testValidateSchemaWithListOfStructsCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "items",
                Types.ListType.ofRequired(
                    3,
                    Types.StructType.of(
                        Types.NestedField.required(4, "name", Types.StringType.get()),
                        Types.NestedField.required(5, "NAME", Types.StringType.get())))));

    Assertions.assertThatThrownBy(() -> validator.validateNoCaseConflicts(schema))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("case-conflicting column names")
        .hasMessageContaining("items.element.NAME");
  }

  @Test
  public void testValidateSchemaWithMapValueStructCaseConflict() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "metadata",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(5, "key", Types.StringType.get()),
                        Types.NestedField.required(6, "KEY", Types.StringType.get())))));

    Assertions.assertThatThrownBy(() -> validator.validateNoCaseConflicts(schema))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("case-conflicting column names")
        .hasMessageContaining("metadata.value.KEY");
  }

  @Test
  public void testValidateSameNameAtDifferentLevelsAllowed() {
    // "id" at top level and "id" inside nested struct should be allowed
    // because they have different full paths: "id" vs "nested.id"
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(3, "id", Types.IntegerType.get()))));

    // Should not throw
    validator.validateNoCaseConflicts(schema);
  }
}
