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
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;
import picocli.CommandLine;

@CommandLine.Command(
    name = "run",
    mixinStandardHelpOptions = true,
    description = {"Run Polaris persistence maintenance."})
public class PersistenceMaintenanceRunCommand extends BaseMaintenanceCommand {
  @Inject MaintenanceService maintenanceService;

  // TODO once there's a fully-tested tasks "client" and 'MaintenanceTaskBehavior', _running_
  //  maintenance should be directed through the tasks-API, giving users the option to run
  //  maintenance "locally" in the admin client or on any polaris server instance, while also
  //  ensuring (via the tasks framework) that only one maintenance run is active at any time.

  @Override
  public Integer call() {
    checkInMemory();

    printMaintenanceConfig();
    var runSpec = printRealmStates();

    var out = spec.commandLine().getOut();
    out.println();
    out.println("Starting maintenance run...");
    out.println(
        "This can run for quite some time, messages may be not be printed immediately, stay patient...");
    out.println();

    var runInformation = maintenanceService.performMaintenance(runSpec);

    printRunInformation(runInformation, false);

    return 0;
  }
}
