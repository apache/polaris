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
package org.apache.polaris;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.apache.polaris.admintool.PolarisAdminTool;
import picocli.CommandLine;

/**
 * Main entry point for Polaris application. Supports both HTTP server mode and CLI admin mode: - No
 * arguments: Start HTTP server - With arguments: Run CLI commands (bootstrap, purge, etc.)
 */
@QuarkusMain
public class PolarisApplication implements QuarkusApplication {

  @Inject CommandLine.IFactory factory;

  /** Main method that detects CLI mode and activates the appropriate Quarkus profile. */
  public static void main(String... args) {
    if (args.length > 0) {
      // CLI mode - activate the 'cli' profile for quieter logging
      System.setProperty("quarkus.profile", "cli");
    }
    Quarkus.run(PolarisApplication.class, args);
  }

  @Override
  public int run(String... args) throws Exception {
    if (args.length == 0) {
      // No arguments - run as HTTP server
      Quarkus.waitForExit();
      return 0;
    } else {
      // Arguments present - run as CLI tool
      return new CommandLine(new PolarisAdminTool(), factory).execute(args);
    }
  }
}
