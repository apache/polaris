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
package org.apache.polaris.admintool.maintenance;

import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import org.apache.polaris.admintool.BaseCommand;
import org.apache.polaris.core.persistence.MaintenanceManager;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "purge-events",
    mixinStandardHelpOptions = true,
    description = "Purge events from storage. Only supported with relational-jdbc backend.")
public class PurgeEventsCommand extends BaseCommand {

  @Inject @Nullable MaintenanceManager maintenanceManager;

  @CommandLine.Option(
      names = {"--retention-days"},
      paramLabel = "<days>",
      description =
          "Number of days to retain. Events older than this are deleted."
              + " If not specified, all events are purged.")
  Integer retentionDays;

  @Override
  public Integer call() {
    if (maintenanceManager == null) {
      spec.commandLine()
          .getErr()
          .println(
              "Event purge is not supported for the current persistence backend. "
                  + "This operation requires relational-jdbc storage.");
      return EXIT_CODE_MAINTENANCE_ERROR;
    }

    try {
      Instant cutoff =
          retentionDays != null
              ? Instant.now().minus(Duration.ofDays(retentionDays))
              : Instant.now();
      maintenanceManager.purgeEvents(cutoff);
      if (retentionDays != null) {
        spec.commandLine()
            .getOut()
            .printf("Events older than %d days purged successfully.%n", retentionDays);
      } else {
        spec.commandLine().getOut().println("All events purged successfully.");
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace(spec.commandLine().getErr());
      spec.commandLine().getErr().println("Maintenance encountered errors during operation.");
      return EXIT_CODE_MAINTENANCE_ERROR;
    }
  }
}
