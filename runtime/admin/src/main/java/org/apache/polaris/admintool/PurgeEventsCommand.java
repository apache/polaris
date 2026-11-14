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

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.polaris.admintool.config.QuarkusPersistenceConfiguration;
import org.apache.polaris.persistence.relational.jdbc.JdbcEventsPurger;
import picocli.CommandLine;

@CommandLine.Command(
    name = "purge-events",
    mixinStandardHelpOptions = true,
    description = {
      "Delete events older than a given age from the events table.",
      "Only supported with the relational-jdbc persistence.",
    })
public class PurgeEventsCommand extends BaseCommand {

  private static final String RELATIONAL_JDBC = "relational-jdbc";

  @Inject QuarkusPersistenceConfiguration persistenceConfiguration;
  @Inject Instance<JdbcEventsPurger> eventsPurger;
  @Inject Clock clock;

  @CommandLine.Option(
      names = {"-r", "--realm"},
      paramLabel = "<realm>",
      required = true,
      description = "The name of a realm whose events to purge.")
  List<String> realms;

  @CommandLine.Option(
      names = {"--older-than"},
      paramLabel = "<duration>",
      required = true,
      description =
          "Delete events older than this ISO-8601 duration, such as P30D (30 days) or PT12H (12"
              + " hours).")
  Duration olderThan;

  @CommandLine.Option(
      names = {"--batch-size"},
      paramLabel = "<count>",
      defaultValue = "1000",
      description =
          "The maximum number of events deleted in a single transaction. Defaults to ${DEFAULT-VALUE}.")
  int batchSize;

  @Override
  public Integer call() {
    var out = spec.commandLine().getOut();
    var err = spec.commandLine().getErr();

    if (!RELATIONAL_JDBC.equals(persistenceConfiguration.type())) {
      err.printf(
          "Purging events is only supported with the %s persistence, not %s.%n",
          RELATIONAL_JDBC, persistenceConfiguration.type());
      return EXIT_CODE_USAGE;
    }
    if (olderThan.isNegative() || olderThan.isZero()) {
      err.println("--older-than must be a positive duration.");
      return EXIT_CODE_USAGE;
    }
    if (batchSize <= 0) {
      err.println("--batch-size must be positive.");
      return EXIT_CODE_USAGE;
    }

    Instant cutoff;
    try {
      cutoff = clock.instant().minus(olderThan);
    } catch (DateTimeException | ArithmeticException e) {
      err.println("--older-than is too large.");
      return EXIT_CODE_USAGE;
    }
    try {
      for (String realm : realms.stream().distinct().toList()) {
        long deleted = eventsPurger.get().purgeEventsBefore(realm, cutoff, batchSize);
        out.printf("Purged %d events older than %s from realm %s.%n", deleted, cutoff, realm);
      }
      out.println("Purge of events completed successfully.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace(err);
      err.println("Purge of events encountered errors during operation.");
      return EXIT_CODE_PURGE_ERROR;
    }
  }
}
