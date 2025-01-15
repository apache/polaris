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
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.PolarisCredentialsBootstrap;
import picocli.CommandLine;

@CommandLine.Command(
    name = "bootstrap",
    mixinStandardHelpOptions = true,
    description = "Bootstraps realms and principal credentials.")
public class BootstrapCommand extends BaseCommand {

  @CommandLine.Option(
      names = {"-r", "--realm"},
      required = true,
      description = "The name of a realm to bootstrap.")
  List<String> realms;

  @CommandLine.Option(
      names = {"-c", "--credential"},
      description =
          "Principal credentials to bootstrap. Must be of the form 'realm,userName,clientId,clientSecret'.")
  List<String> credentials;

  @Override
  public Integer call() {
    warnOnInMemory();

    PolarisCredentialsBootstrap credentialsBootstrap =
        credentials == null || credentials.isEmpty()
            ? PolarisCredentialsBootstrap.EMPTY
            : PolarisCredentialsBootstrap.fromList(credentials);

    // Execute the bootstrap
    Map<String, PrincipalSecretsResult> results =
        metaStoreManagerFactory.bootstrapRealms(realms, credentialsBootstrap);

    // Log any errors:
    boolean success = true;
    for (Map.Entry<String, PrincipalSecretsResult> result : results.entrySet()) {
      if (!result.getValue().isSuccess()) {
        String realm = result.getKey();
        spec.commandLine()
            .getErr()
            .printf(
                "Bootstrapping '%s' failed: %s%n",
                realm, result.getValue().getReturnStatus().toString());
        success = false;
      }
    }

    if (success) {
      spec.commandLine().getOut().println("Bootstrap completed successfully.");
      return 0;
    } else {
      spec.commandLine().getErr().println("Bootstrap encountered errors during operation.");
      return EXIT_CODE_BOOTSTRAP_ERROR;
    }
  }
}
