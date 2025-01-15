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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
class TestJavaVM {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void checkJavaVersionStrings() {
    soft.assertThat(JavaVM.majorVersionFromString("11")).isEqualTo(11);
    soft.assertThat(JavaVM.majorVersionFromString("17.0.1")).isEqualTo(17);
    soft.assertThat(JavaVM.majorVersionFromString("1.8.0-foo+bar")).isEqualTo(8);
  }

  @Test
  void checkResolveEnvJdkHomeLinux() {
    var env = new HashMap<String, String>();
    env.put("JDK11_HOME", "/mycomputer/java11");
    env.put("JAVA17_HOME", "/mycomputer/java17");

    var sysProps = new HashMap<String, String>();
    sysProps.put("jdk9.home", "/mycomputer/java9");
    sysProps.put("java10.home", "/mycomputer/java10");
    sysProps.put("os.name", "Linux");

    soft.assertThat(JavaVM.locateJavaHome(8, env::get, sysProps::get, i -> "/hello/there"))
        .isNull();
    soft.assertThat(JavaVM.locateJavaHome(9, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java9");
    soft.assertThat(JavaVM.locateJavaHome(10, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java10");
    soft.assertThat(JavaVM.locateJavaHome(11, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java11");
    soft.assertThat(JavaVM.locateJavaHome(14, env::get, sysProps::get, i -> "/hello/there"))
        .isNull();
    soft.assertThat(JavaVM.locateJavaHome(17, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java17");
  }

  @Test
  void checkResolveEnvJdkHomeMacOS() {
    var env = new HashMap<String, String>();
    env.put("JDK11_HOME", "/mycomputer/java11");
    env.put("JAVA17_HOME", "/mycomputer/java17");

    var sysProps = new HashMap<String, String>();
    sysProps.put("jdk9.home", "/mycomputer/java9");
    sysProps.put("java10.home", "/mycomputer/java10");
    sysProps.put("os.name", "Darwin");

    soft.assertThat(
            JavaVM.locateJavaHome(
                8, env::get, sysProps::get, i -> i == 8 ? "/from_java_home/v8" : null))
        .isEqualTo("/from_java_home/v8");
    soft.assertThat(JavaVM.locateJavaHome(9, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java9");
    soft.assertThat(JavaVM.locateJavaHome(10, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java10");
    soft.assertThat(JavaVM.locateJavaHome(11, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java11");
    soft.assertThat(
            JavaVM.locateJavaHome(
                14, env::get, sysProps::get, i -> i >= 12 ? "/from_java_home/v16" : null))
        .isEqualTo("/from_java_home/v16");
    soft.assertThat(
            JavaVM.locateJavaHome(
                17, env::get, sysProps::get, i -> i >= 12 ? "/from_java_home/v8" : null))
        .isEqualTo("/mycomputer/java17");
  }

  @Test
  void checkJreResolve(@TempDir Path jdkDir) throws Exception {
    var jdkBinDir = jdkDir.resolve("bin");
    var jdkJavaFile = jdkBinDir.resolve(JavaVM.executableName("java"));
    var jreDir = jdkDir.resolve("jre");
    var jreBinDir = jreDir.resolve("bin");
    var jreJavaFile = jreBinDir.resolve(JavaVM.executableName("java"));

    Files.createDirectories(jdkBinDir);
    Files.createDirectories(jreBinDir);
    Files.createFile(
        jreJavaFile,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));

    var env = new HashMap<String, String>();
    env.put("JDK8_HOME", jreDir.toString());

    var sysProps = new HashMap<String, String>();
    sysProps.put("os.name", "Linux");

    soft.assertThat(JavaVM.locateJavaHome(8, env::get, sysProps::get, x -> null))
        .isEqualTo(jreDir.toString());
    soft.assertThat(JavaVM.fixJavaHome(jreDir)).isEqualTo(jreDir);
    soft.assertThat(JavaVM.forJavaHome(jreDir).getJavaExecutable()).isEqualTo(jreJavaFile);
    soft.assertThat(JavaVM.forJavaHome(jreDir).getJavaHome()).isEqualTo(jreDir);
    soft.assertThat(JavaVM.forJavaHome(jreDir.toString()).getJavaHome()).isEqualTo(jreDir);

    Files.createFile(
        jdkJavaFile,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));

    soft.assertThat(JavaVM.fixJavaHome(jreDir)).isEqualTo(jdkDir);
    soft.assertThat(JavaVM.forJavaHome(jreDir).getJavaExecutable()).isEqualTo(jdkJavaFile);
    soft.assertThat(JavaVM.forJavaHome(jreDir).getJavaHome()).isEqualTo(jdkDir);
    soft.assertThat(JavaVM.forJavaHome(jreDir.toString()).getJavaHome()).isEqualTo(jdkDir);
  }
}
