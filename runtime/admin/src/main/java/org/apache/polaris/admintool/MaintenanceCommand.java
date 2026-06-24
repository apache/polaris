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
import org.apache.polaris.core.persistence.MaintenanceManager;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "maintenance",
    mixinStandardHelpOptions = true,
    description = "Performs maintenance operations on Polaris storage.")
public class MaintenanceCommand extends BaseCommand {

  @Inject @Nullable MaintenanceManager maintenanceManager;

  @CommandLine.Option(
      names = {"--purge-events"},
      description = "Purge all events from storage. Only supported with relational-jdbc backend.")
  boolean purgeEvents;

  @Override
  public Integer call() {
    if (!purgeEvents) {
      spec.commandLine().getErr().println("No maintenance operation specified.");
      spec.commandLine().usage(spec.commandLine().getOut());
      return EXIT_CODE_USAGE;
    }

    if (maintenanceManager == null) {
      spec.commandLine()
          .getErr()
          .println(
              "Event purge is not supported for the current persistence backend. "
                  + "This operation requires relational-jdbc storage.");
      return EXIT_CODE_MAINTENANCE_ERROR;
    }

    try {
      maintenanceManager.purgeEvents();
      spec.commandLine().getOut().println("Events purged successfully.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace(spec.commandLine().getErr());
      spec.commandLine().getErr().println("Maintenance encountered errors during operation.");
      return EXIT_CODE_MAINTENANCE_ERROR;
    }
  }
}
