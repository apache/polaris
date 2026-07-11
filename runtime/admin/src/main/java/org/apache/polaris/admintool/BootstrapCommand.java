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
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableBootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import picocli.CommandLine;

@CommandLine.Command(
    name = "bootstrap",
    mixinStandardHelpOptions = true,
    description = "Bootstraps realms and root principal credentials.")
public class BootstrapCommand extends BaseMetaStoreCommand {

  @CommandLine.Mixin InputOptions inputOptions;

  static class InputOptions {

    // This ArgGroup enforces the mandatory, exclusive choice.
    @CommandLine.ArgGroup(multiplicity = "1")
    RootCredentialsOptions rootCredentialsOptions;

    // This static inner class encapsulates the mutually exclusive choices.
    static class RootCredentialsOptions {

      @CommandLine.ArgGroup(exclusive = false, heading = "Standard Input Options:%n")
      StandardInputOptions stdinOptions;

      @CommandLine.ArgGroup(exclusive = false, heading = "File Input Options:%n")
      FileInputOptions fileOptions;
    }

    // Option container classes
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
      Iterable<String> realms;

      if (inputOptions.rootCredentialsOptions.fileOptions != null) {
        rootCredentialsSet =
            RootCredentialsSet.fromUri(
                inputOptions.rootCredentialsOptions.fileOptions.file.toUri());
        realms = rootCredentialsSet.credentials().keySet();
      } else {
        realms = inputOptions.rootCredentialsOptions.stdinOptions.realms;
        rootCredentialsSet =
            inputOptions.rootCredentialsOptions.stdinOptions.credentials == null
                    || inputOptions.rootCredentialsOptions.stdinOptions.credentials.isEmpty()
                ? RootCredentialsSet.EMPTY
                : RootCredentialsSet.fromList(
                    inputOptions.rootCredentialsOptions.stdinOptions.credentials);
        if (inputOptions.rootCredentialsOptions.stdinOptions.credentials == null
            || inputOptions.rootCredentialsOptions.stdinOptions.credentials.isEmpty()) {
          if (!inputOptions.rootCredentialsOptions.stdinOptions.printCredentials) {
            spec.commandLine()
                .getErr()
                .println(
                    "Specify either `--credentials` or `--print-credentials` to ensure"
                        + " the root user is accessible after bootstrapping.");
            return EXIT_CODE_BOOTSTRAP_ERROR;
          }
        }
      }

      BootstrapOptions bootstrapOptions =
          ImmutableBootstrapOptions.builder()
              .realms(realms)
              .rootCredentialsSet(rootCredentialsSet)
              .schemaOptions(ImmutableSchemaOptions.builder().build())
              .build();

      // Execute the bootstrap
      Map<String, PrincipalSecretsResult> results =
          metaStoreManagerFactory.bootstrapRealms(bootstrapOptions);

      // Log any errors:
      boolean success = true;
      int alreadyBootstrappedCount = 0;
      for (Map.Entry<String, PrincipalSecretsResult> result : results.entrySet()) {
        if (result.getValue().isSuccess()) {
          String realm = result.getKey();
          spec.commandLine().getOut().printf("Realm '%s' successfully bootstrapped.%n", realm);
          if (inputOptions.rootCredentialsOptions.stdinOptions != null
              && inputOptions.rootCredentialsOptions.stdinOptions.printCredentials) {
            String msg =
                String.format(
                    "realm: %1$s root principal credentials: %2$s:%3$s",
                    result.getKey(),
                    result.getValue().getPrincipalSecrets().getPrincipalClientId(),
                    result.getValue().getPrincipalSecrets().getMainSecret());
            spec.commandLine().getOut().println(msg);
          }
        } else if (result.getValue().alreadyExists()) {
          // Re-running bootstrap on an existing realm is a no-op, not an error; this
          // keeps automated bootstrap jobs idempotent. Existing credentials are never
          // returned or altered.
          alreadyBootstrappedCount++;
          String realm = result.getKey();
          spec.commandLine()
              .getOut()
              .printf("Realm '%s' is already bootstrapped; skipping.%n", realm);
        } else {
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
        if (alreadyBootstrappedCount > 0) {
          spec.commandLine()
              .getOut()
              .printf(
                  "Bootstrap completed (%d realm(s) already bootstrapped).%n",
                  alreadyBootstrappedCount);
        } else {
          spec.commandLine().getOut().println("Bootstrap completed successfully.");
        }
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
