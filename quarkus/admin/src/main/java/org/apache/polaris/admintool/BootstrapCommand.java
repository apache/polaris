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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import picocli.CommandLine;

@CommandLine.Command(
    name = "bootstrap",
    mixinStandardHelpOptions = true,
    description = "Bootstraps realms and root principal credentials.")
public class BootstrapCommand extends BaseCommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  InputOptions inputOptions;

  static class InputOptions {

    @CommandLine.ArgGroup(multiplicity = "1", exclusive = false)
    StandardInputOptions stdinOptions;

    @CommandLine.ArgGroup(multiplicity = "1")
    FileInputOptions fileOptions;

    static class StandardInputOptions {

      @CommandLine.Option(
          names = {"-r", "--realm"},
          paramLabel = "<realm>",
          required = true,
          description = "The name of a realm to bootstrap.")
      List<String> realms;

      @CommandLine.Option(
          names = {"-c", "--credential"},
          paramLabel = "<realm,clientId,clientSecret>",
          description =
              "Root principal credentials to bootstrap. Must be of the form 'realm,clientId,clientSecret'.")
      List<String> credentials;

      @CommandLine.Option(
          names = {"-p", "--print-credentials"},
          description = "Print root credentials to stdout")
      boolean printCredentials;
    }

    static class FileInputOptions {
      @CommandLine.Option(
          names = {"-f", "--credentials-file"},
          paramLabel = "<file>",
          description = "A file containing root principal credentials to bootstrap.")
      Path file;
    }
  }

  @Override
  public Integer call() {
    try {
      RootCredentialsSet rootCredentialsSet;
      List<String> realms; // TODO Iterable

      if (inputOptions.fileOptions != null) {
        rootCredentialsSet =
            RootCredentialsSet.fromUrl(inputOptions.fileOptions.file.toUri().toURL());
        realms = rootCredentialsSet.credentials().keySet().stream().toList();
      } else {
        realms = inputOptions.stdinOptions.realms;
        rootCredentialsSet =
            inputOptions.stdinOptions.credentials == null
                    || inputOptions.stdinOptions.credentials.isEmpty()
                ? RootCredentialsSet.EMPTY
                : RootCredentialsSet.fromList(inputOptions.stdinOptions.credentials);
        if (rootCredentialsSet.credentials().isEmpty()
            && !inputOptions.stdinOptions.printCredentials) {
          spec.commandLine()
              .getErr()
              .println(
                  "Specify either `--credentials` or `--print-credentials` to ensure"
                      + " the root user is accessible after bootstrapping.");
          return EXIT_CODE_BOOTSTRAP_ERROR;
        }
      }

      Map<String, PrincipalSecretsResult> results =
          metaStoreManagerFactory.bootstrapRealms(realms, rootCredentialsSet);

      boolean success = true;
      for (Map.Entry<String, PrincipalSecretsResult> result : results.entrySet()) {
        String realm = result.getKey();
        PrincipalSecretsResult secretsResult = result.getValue();
        if (secretsResult.isSuccess()) {
          spec.commandLine().getOut().printf("Realm '%s' successfully bootstrapped.%n", realm);
          if (inputOptions.stdinOptions != null
              && inputOptions.stdinOptions.printCredentials
              && !rootCredentialsSet.credentials().containsKey(realm)) {
            spec.commandLine()
                .getOut()
                .printf(
                    "realm: %s root principal credentials: %s:%s%n",
                    realm,
                    secretsResult.getPrincipalSecrets().getPrincipalClientId(),
                    secretsResult.getPrincipalSecrets().getMainSecret());
          }
        } else {
          spec.commandLine()
              .getErr()
              .printf("Bootstrapping '%s' failed: %s%n", realm, secretsResult.getReturnStatus());
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
    } catch (Exception e) {
      e.printStackTrace(spec.commandLine().getErr());
      spec.commandLine().getErr().println("Bootstrap encountered errors during operation.");
      return EXIT_CODE_BOOTSTRAP_ERROR;
    }
  }
}
