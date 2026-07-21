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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.polaris.admintool.config.QuarkusPersistenceConfiguration;
import org.apache.polaris.persistence.relational.jdbc.RealmPurgeService;
import picocli.CommandLine;

@CommandLine.Command(
    name = "purge-drain",
    mixinStandardHelpOptions = true,
    description = {
      "Drain queued realm purges (relational-JDBC backend).",
      "Deletes purged realms' time-series data (events and metrics reports) in bounded batches. "
          + "Safe to run repeatedly and concurrently; intended to be scheduled (e.g. as a "
          + "periodic job)."
    })
public class PurgeDrainCommand extends BaseCommand {

  private static final String JDBC_PERSISTENCE_TYPE = "relational-jdbc";

  @Inject QuarkusPersistenceConfiguration persistenceConfiguration;
  @Inject RealmPurgeService realmPurgeService;

  @CommandLine.Option(
      names = {"-r", "--realm"},
      paramLabel = "<realm>",
      description = "The name of a realm to drain; all queued realms are drained by default.")
  List<String> realms = List.of();

  @CommandLine.Option(
      names = "--batch-size",
      defaultValue = "1000",
      description = "Maximum rows deleted per statement (default: ${DEFAULT-VALUE}).")
  int batchSize;

  @CommandLine.Option(
      names = "--claim-lease-ms",
      defaultValue = "600000",
      description =
          "Milliseconds a drain claim is honored before another run may resume an interrupted "
              + "realm (default: ${DEFAULT-VALUE}).")
  long claimLeaseMs;

  @Override
  public Integer call() {
    if (batchSize <= 0) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "--batch-size must be strictly positive.");
    }
    if (claimLeaseMs <= 0) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "--claim-lease-ms must be strictly positive.");
    }
    // Other backends reclaim this data through their own maintenance; there is no queue to drain.
    if (!JDBC_PERSISTENCE_TYPE.equals(persistenceConfiguration.type())) {
      spec.commandLine()
          .getErr()
          .printf(
              "purge-drain applies to the '%s' persistence type, but '%s' is configured.%n",
              JDBC_PERSISTENCE_TYPE, persistenceConfiguration.type());
      return EXIT_CODE_USAGE;
    }
    var out = spec.commandLine().getOut();
    int drained =
        realmPurgeService.drainOnce(
            localNodeId(), batchSize, claimLeaseMs, realms, line -> out.println(line));
    out.printf("Drained %d realm purge(s).%n", drained);
    return 0;
  }

  /**
   * Claim owner recorded on the realms this run drains: the host name and the process id, so that a
   * claim left behind by an interrupted run points at the run that took it.
   */
  private static String localNodeId() {
    return hostName() + "-" + ProcessHandle.current().pid();
  }

  private static String hostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }
}
