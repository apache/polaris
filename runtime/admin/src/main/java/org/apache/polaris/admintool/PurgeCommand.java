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

import java.util.List;
import java.util.Map;
import picocli.CommandLine;

@CommandLine.Command(
    name = "purge",
    mixinStandardHelpOptions = true,
    description = "Purge realms and all associated entities.")
public class PurgeCommand extends BaseCommand {

  @CommandLine.Option(
      names = {"-r", "--realm"},
      paramLabel = "<realm>",
      required = true,
      description = "The name of a realm to purge.")
  List<String> realms;

  @Override
  public Integer call() {
    try {
      var result = metaStoreManagerFactory.purgeRealms(realms);
      var failed =
          result.entrySet().stream()
              .filter(e -> !e.getValue().isSuccess())
              .map(Map.Entry::getKey)
              .toList();
      if (failed.isEmpty()) {
        spec.commandLine().getOut().println("Purge completed successfully.");
        return 0;
      }

      var out = spec.commandLine().getOut();
      failed.forEach(
          r ->
              out.printf(
                  "Realm %s is not bootstrapped, could not load root principal. Please run Bootstrap command.%n",
                  r));

      spec.commandLine().getErr().printf("Purge encountered errors during operation.");
      return EXIT_CODE_PURGE_ERROR;
    } catch (Exception e) {
      spec.commandLine().getErr().println("Purge encountered errors during operation.");
      return EXIT_CODE_PURGE_ERROR;
    }
  }
}
