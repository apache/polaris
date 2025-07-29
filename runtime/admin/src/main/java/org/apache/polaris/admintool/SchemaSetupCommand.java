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

package org.apache.polaris.admintool;

import jakarta.inject.Inject;
import org.apache.polaris.admintool.BootstrapCommand.InputOptions.SchemaInputOptions;
import org.apache.polaris.core.persistence.SchemaInitializer;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(
    name = "setup",
    mixinStandardHelpOptions = true,
    description = "Set up the polaris schema for Polaris metadata storage")
public class SchemaSetupCommand extends BaseCommand {

  @Inject SchemaInitializer schemaInitializer;

  @ArgGroup(exclusive = false)
  public SchemaInputOptions schemaInputOptions;

  @Override
  public Integer call() {
    if (schemaInputOptions == null) {
      return executeWith(ImmutableSchemaOptions.builder().build());
    }

    if (!schemaInputOptions.setupSchema) {
      return reportFailure("Schema initialization is disabled.");
    }

    boolean hasExplicitSchema =
        schemaInputOptions.schemaVersion != null
            || (schemaInputOptions.schemaFile != null && !schemaInputOptions.schemaFile.isEmpty());

    SchemaOptions schemaOptions =
        hasExplicitSchema
            ? ImmutableSchemaOptions.builder()
                .schemaVersion(schemaInputOptions.schemaVersion)
                .schemaFile(schemaInputOptions.schemaFile)
                .build()
            : ImmutableSchemaOptions.builder().build();

    return executeWith(schemaOptions);
  }

  private int executeWith(SchemaOptions schemaOptions) {
    return schemaInitializer.setupSchema(schemaOptions)
        ? reportSuccess()
        : reportFailure("Failed to initialize the schema.");
  }

  private int reportSuccess() {
    spec.commandLine().getOut().println("Schema initialization completed successfully.");
    return 0;
  }

  private int reportFailure(String message) {
    spec.commandLine().getErr().println(message);
    return EXIT_CODE_SCHEMA_ERROR;
  }
}
