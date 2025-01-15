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
package org.apache.polaris.apprunner.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Pattern;

/**
 * Helper class to locate a JDK by Java major version and return the path to the {@code java}
 * executable.
 */
public final class JavaVM {

  public static final int MAX_JAVA_VERSION_TO_CHECK = 19;

  private static final Pattern MAJOR_VERSION_PATTERN = Pattern.compile("^(1[.])?([0-9]+)([.].+)?$");

  private static final AtomicReference<JavaVM> CURRENT_JVM = new AtomicReference<>();
  private final Path javaHome;

  static String locateJavaHome(
      int majorVersion,
      Function<String, String> getenv,
      Function<String, String> getProperty,
      IntFunction<String> macosHelper) {

    var home = getenv.apply(String.format("JDK%d_HOME", majorVersion));
    if (home != null) {
      return home;
    }
    home = getenv.apply(String.format("JAVA%d_HOME", majorVersion));
    if (home != null) {
      return home;
    }

    home = getProperty.apply(String.format("jdk%d.home", majorVersion));
    if (home != null) {
      return home;
    }
    home = getProperty.apply(String.format("java%d.home", majorVersion));
    if (home != null) {
      return home;
    }

    if (getProperty.apply("os.name").toLowerCase(Locale.ROOT).contains("darwin")) {
      return macosHelper.apply(majorVersion);
    }

    return null;
  }

  /**
   * Loops from {@code majorVersion} up to {@value #MAX_JAVA_VERSION_TO_CHECK} until a call to
   * {@link #findJavaVMTryExact(int)} returns a Java-Home.
   *
   * <p>Returns the current JVM from {@link #getCurrentJavaVM()}, if its major version is greater
   * than or equal to the requested {@code majorVersion}.
   *
   * @param majorVersion the Java major-version to start with.
   * @return a Java-VM on the local system or {@code null}, if no matching Java-Home could be found.
   */
  public static JavaVM findJavaVM(int majorVersion) {
    if (currentJavaVMMajorVersion() >= majorVersion) {
      return getCurrentJavaVM();
    }
    for (var i = majorVersion; i < MAX_JAVA_VERSION_TO_CHECK; i++) {
      var jvm = findJavaVMTryExact(i);
      if (jvm != null) {
        return jvm;
      }
    }
    return null;
  }

  /**
   * Find a Java-Home that exactly matches the given Java major version.
   *
   * <p>Searches for the Java-Home in these places in this exact order:
   *
   * <ol>
   *   <li>Environment variable {@code JDKxx_HOME}, where {@code xx} is the {@code majorVersion}.
   *   <li>Environment variable {@code JAVAxx_HOME}, where {@code xx} is the {@code majorVersion}.
   *   <li>System property {@code jdkXX.home}, where {@code XX} is the {@code majorVersion}.
   *   <li>System property {@code javaXX.home}, where {@code XX} is the {@code majorVersion}.
   *   <li>Using the {@code /usr/libexec/java_home} on MacOS, which may return a newer Java version.
   * </ol>
   *
   * @param majorVersion the Java major-version to search for.
   * @return a Java-VM on the local system or {@code null}, if no matching Java-Home could be found.
   */
  public static JavaVM findJavaVMTryExact(int majorVersion) {
    if (majorVersion == currentJavaVMMajorVersion()) {
      return getCurrentJavaVM();
    }

    var home =
        locateJavaHome(
            majorVersion,
            System::getenv,
            System::getProperty,
            ver -> {
              try {
                String versionArg = ver < 9 ? ("1." + ver) : Integer.toString(ver);
                Process proc =
                    new ProcessBuilder()
                        .command("/usr/libexec/java_home", "-v", versionArg)
                        .start();
                return new BufferedReader(
                        new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()))
                    .readLine();
              } catch (IOException e) {
                return null;
              }
            });
    if (home != null) {
      return forJavaHome(home);
    }
    return null;
  }

  /**
   * Get the {@link JavaVM} instance for the current JVM.
   *
   * @return {@link JavaVM} instance for the current JVM, never {@code null}.
   */
  public static JavaVM getCurrentJavaVM() {
    var current = CURRENT_JVM.get();
    if (current == null) {
      CURRENT_JVM.set(current = forJavaHome(Paths.get(System.getProperty("java.home"))));
    }
    return current;
  }

  public static int currentJavaVMMajorVersion() {
    return majorVersionFromString(System.getProperty("java.version"));
  }

  /**
   * Extracts the major version from a Java-version-string as returned from {@code
   * System.getProperty("java.version)}.
   *
   * @param versionString the Java-version-string
   * @return extracted Java major version
   */
  public static int majorVersionFromString(String versionString) {
    var m = MAJOR_VERSION_PATTERN.matcher(versionString);
    if (!m.matches()) {
      throw new IllegalArgumentException(
          String.format("%s is not a valid Java version string", versionString));
    }
    return Integer.parseInt(m.group(2));
  }

  static Path fixJavaHome(Path javaHome) {
    if ("jre".equals(javaHome.getFileName().toString())
        && Files.isExecutable(javaHome.resolve("bin").resolve(executableName("java")))) {
      var check = javaHome.getParent();
      if (Files.isExecutable(check.resolve("bin").resolve(executableName("java")))) {
        javaHome = check;
      }
    }
    return javaHome;
  }

  public static JavaVM forJavaHome(String javaHome) {
    return forJavaHome(Paths.get(javaHome));
  }

  public static JavaVM forJavaHome(Path javaHome) {
    return new JavaVM(fixJavaHome(javaHome));
  }

  private JavaVM(Path javaHome) {
    this.javaHome = javaHome;
  }

  public Path getJavaHome() {
    return javaHome;
  }

  public Path getJavaExecutable() {
    return getExecutable("java");
  }

  private Path getExecutable(String executable) {
    return javaHome.resolve("bin").resolve(executableName(executable));
  }

  static String executableName(String executable) {
    if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) {
      return executable + ".exe";
    }
    return executable;
  }
}
