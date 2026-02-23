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
import java.util.List;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;
import picocli.CommandLine;

/**
 * CLI command to bootstrap the metrics schema independently from the entity schema.
 *
 * <p>This command allows operators to add metrics persistence support to an existing Polaris
 * deployment without re-bootstrapping the entity schema. It supports both fresh installation and
 * upgrading from an older schema version to a newer one.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * polaris-admin bootstrap-metrics -r my-realm
 * polaris-admin bootstrap-metrics -r realm1 -r realm2
 * polaris-admin bootstrap-metrics -r my-realm --version 2
 * }</pre>
 */
@CommandLine.Command(
    name = "bootstrap-metrics",
    mixinStandardHelpOptions = true,
    description = "Bootstraps or upgrades the metrics schema for existing realms.")
public class BootstrapMetricsCommand extends BaseCommand {

  @Inject MetricsSchemaBootstrap metricsSchemaBootstrap;

  @CommandLine.Option(
      names = {"-r", "--realm"},
      paramLabel = "<realm>",
      required = true,
      description = "The name of a realm to bootstrap metrics for.")
  List<String> realms;

  @CommandLine.Option(
      names = {"-v", "--version"},
      paramLabel = "<version>",
      description = "The metrics schema version to bootstrap/upgrade to (default: latest).")
  Integer version;

  @Override
  public Integer call() {
    boolean success = true;
    int targetVersion = (version != null) ? version : metricsSchemaBootstrap.getLatestVersion();

    for (String realm : realms) {
      try {
        int currentVersion = metricsSchemaBootstrap.getCurrentVersion(realm);
        if (currentVersion >= targetVersion) {
          spec.commandLine()
              .getOut()
              .printf(
                  "Metrics schema already at version %d (target: %d) for realm '%s'. Skipping.%n",
                  currentVersion, targetVersion, realm);
        } else if (currentVersion == 0) {
          spec.commandLine()
              .getOut()
              .printf("Bootstrapping metrics schema v%d for realm '%s'...%n", targetVersion, realm);
          metricsSchemaBootstrap.bootstrap(realm);
          spec.commandLine()
              .getOut()
              .printf(
                  "Metrics schema v%d successfully bootstrapped for realm '%s'.%n",
                  targetVersion, realm);
        } else {
          spec.commandLine()
              .getOut()
              .printf(
                  "Upgrading metrics schema from v%d to v%d for realm '%s'...%n",
                  currentVersion, targetVersion, realm);
          metricsSchemaBootstrap.bootstrap(realm);
          spec.commandLine()
              .getOut()
              .printf(
                  "Metrics schema successfully upgraded to v%d for realm '%s'.%n",
                  targetVersion, realm);
        }
      } catch (Exception e) {
        spec.commandLine()
            .getErr()
            .printf(
                "Failed to bootstrap metrics schema for realm '%s': %s%n", realm, e.getMessage());
        e.printStackTrace(spec.commandLine().getErr());
        success = false;
      }
    }

    if (success) {
      spec.commandLine().getOut().println("Metrics bootstrap completed successfully.");
      return 0;
    } else {
      spec.commandLine().getErr().println("Metrics bootstrap encountered errors during operation.");
      return EXIT_CODE_BOOTSTRAP_ERROR;
    }
  }
}
