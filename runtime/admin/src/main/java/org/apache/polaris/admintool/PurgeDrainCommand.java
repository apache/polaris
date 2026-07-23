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
public class PurgeDrainCommand extends BaseMetaStoreCommand {

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
    int drained = metaStoreManagerFactory.drainRealmPurges(batchSize, claimLeaseMs);
    spec.commandLine().getOut().printf("Drained %d realm purge(s).%n", drained);
    return 0;
  }
}
