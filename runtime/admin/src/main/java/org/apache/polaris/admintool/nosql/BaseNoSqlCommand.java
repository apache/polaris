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
package org.apache.polaris.admintool.nosql;

import jakarta.inject.Inject;
import org.apache.polaris.admintool.BaseCommand;
import org.apache.polaris.persistence.nosql.api.backend.Backend;

public abstract class BaseNoSqlCommand extends BaseCommand {
  @Inject protected Backend backend;

  protected void checkInMemory() {
    if ("InMemory".equals(backend.type())) {
      var err = spec.commandLine().getErr();

      err.println();
      err.println("Running persistence-maintenance against InMemory is useless...");
      err.println();
    }
  }

  protected void printNoSqlInfo() {
    var out = spec.commandLine().getOut();

    out.println("Polaris NoSql persistence has multiple subcommands,");
    out.println("use the 'help nosql' command.");
    out.println();

    checkInMemory();

    out.println();
    out.println("Information: selected NoSql persistence backend: " + backend.type());
  }
}
