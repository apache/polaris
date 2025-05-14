/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.docs.generator;

import static javax.tools.DocumentationTool.Location.DOCUMENTATION_OUTPUT;
import static javax.tools.JavaFileObject.Kind.SOURCE;
import static javax.tools.StandardLocation.CLASS_PATH;
import static javax.tools.StandardLocation.SOURCE_PATH;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.tools.DiagnosticListener;
import javax.tools.DocumentationTool;
import javax.tools.JavaFileObject;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Tool to run {@link DocGenDoclet}, the markdown docs generation.
 *
 * <p>This is built as a separate tool, because running {@link DocGenDoclet} inside {@code javadoc}
 * leads to wrong and incomplete results for smallrye-config documentation due to class loader
 * isolation issues. {@code javadoc} uses a separate {@link ClassLoader} for the doclet, which
 * breaks the proper inspection via smallrye-config's using {@link
 * io.smallrye.config.ConfigMappingInterface}: the default values are missing, properties from
 * supertypes are missing and the property names are usually wrong.
 *
 * <p>This separate tool approach makes the integration into Gradle easier, too.
 */
@Command(
    name = "generate",
    mixinStandardHelpOptions = true,
    description = "Generate markdown documentation")
public class ReferenceConfigDocsGenerator implements Callable<Integer> {
  @Option(
      names = {"-cp", "--classpath"},
      arity = "*",
      split = ":")
  List<Path> classpath = new ArrayList<>();

  @Option(
      names = {"-sp", "--sourcepath"},
      arity = "*",
      split = ":")
  List<Path> sourcepath = new ArrayList<>();

  @Option(
      names = {"-d", "--destination"},
      arity = "1",
      required = true)
  Path output;

  @Option(names = {"-v", "--verbose"})
  boolean verbose;

  public ReferenceConfigDocsGenerator() {}

  public ReferenceConfigDocsGenerator(
      List<Path> sourcepath, List<Path> classpath, Path output, boolean verbose) {
    this.classpath = classpath;
    this.sourcepath = sourcepath;
    this.output = output;
    this.verbose = verbose;
  }

  @Override
  public Integer call() throws Exception {
    var docTool =
        ServiceLoader.load(DocumentationTool.class)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No DocumentationTool instance present"));

    Files.createDirectories(output);

    var fileManager = docTool.getStandardFileManager(null, null, null);
    fileManager.setLocationFromPaths(SOURCE_PATH, sourcepath);
    fileManager.setLocationFromPaths(CLASS_PATH, classpath);
    fileManager.setLocationFromPaths(DOCUMENTATION_OUTPUT, List.of(output));

    var sourceFiles = fileManager.list(SOURCE_PATH, "", Set.of(SOURCE), true);

    var options =
        List.of(
            "--ignore-source-errors",
            "-Xmaxwarns",
            verbose ? "100" : "1",
            "-Xmaxerrs",
            verbose ? "100" : "1",
            "-d",
            output.toString());

    DiagnosticListener<JavaFileObject> diagnostics = verbose ? null : diagnostic -> {};
    var out = verbose ? null : Writer.nullWriter();

    var docTask =
        docTool.getTask(out, fileManager, diagnostics, DocGenDoclet.class, options, sourceFiles);

    var result = docTask.call();

    return result ? 0 : 1;
  }

  public static void main(String[] args) {
    var tool = new ReferenceConfigDocsGenerator();
    var commandLine =
        new CommandLine(tool)
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  // Print the full stack trace in all other cases.
                  cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  return cmd.getExitCodeExceptionMapper() != null
                      ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                      : cmd.getCommandSpec().exitCodeOnExecutionException();
                });
    System.exit(commandLine.execute(args));
  }
}
