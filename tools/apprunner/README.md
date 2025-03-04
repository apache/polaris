<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Polaris Apprunner Gradle and Maven Plugins

Gradle and Maven plugins to run a Polaris process and "properly" terminate it for integration testing.

## Java integration tests

Tests that run via a Gradle `Test` type task, "decorated" with the Polaris Apprunner plugin, have access to
four system properties. Integration tests using the Maven plugin have access to the same system properties.
The names of the system properties can be changed, if needed. See the [Gradle Kotlin DSL](#kotlin-dsl--all-in-one)
and [Maven](#maven) sections below for a summary of the available options.

* `quarkus.http.test-port` the port on which the Quarkus server listens for application HTTP requests
* `quarkus.management.test-port` the URL on which the Quarkus server listens for management HTTP requests, this
  URL is the one emitted by Quarkus during startup and will contain `0.0.0.0` as the host.
* `quarkus.http.test-url` the port on which the Quarkus server listens for application HTTP requests
* `quarkus.management.test-url` the URL on which the Quarkus server listens for management HTTP requests, this
  URL is the one emitted by Quarkus during startup and will contain `0.0.0.0` as the host.

The preferred way to get the URI/URL for application HTTP requests is to get the `quarkus.http.test-port` system
property and construct the URI against `127.0.0.1` (or `::1` if you prefer).

```java
public class ITWorksWithPolaris {
    static final URI POLARIS_SERVER_URI =
            URI.create(
                    String.format(
                            "http://127.0.0.1:%s/",
                            requireNonNull(
                                    System.getProperty("quarkus.http.test-port"),
                                    "Required system property quarkus.http.test-port is not set")));

    @Test
    public void pingNessie() {
        // Use the POLARIS_SERVER_URI in your tests ...
    }
}
```

## Gradle

The Polaris Apprunner Gradle ensures that the Polaris Quarkus Server is up and running if and when the configured
test tasks run. It also ensures, as long as you do not forcibly kill Gradle processes, that the Polaris Quarkus Server
is shutdown after the configured test task has finished. Each configured test task gets its "own" Polaris Quarkus
Server started up.

It is possible to configure multiple tasks/test-suites within a Gradle project to run with a Polaris Quarkus Server.
Since tasks of the same Gradle project do not run concurrently (as of today), there are should be no conflicts, except
potentially the working directory.

### Kotlin DSL / step by step

`build.gradle.kts`

1. add the plugin
    ```kotlin
    plugins {
      // Replace the version with a binary release of Polaris.
      // ATTENTION! Within the Polaris repository do _NOT_ add the plugin version, it's implicit !
      id("org.apache.polaris.apprunner") version "0.0.0"
    }
    ```
2. Add the Polaris Quarkus Server as a dependency
    ```kotlin
    dependencies {
      // Tell the plugin which project and configuration it shall 'pull' the Quarkus server from
      polarisQuarkusServer(project(":polaris-quarkus-server", "quarkusRunner"))
    }
    ```
3. If necessary, add a separate test suite
    ```kotlin
    testing {
      suites {
        val polarisServerTest by registering(JvmTestSuite::class) {
          // more test-suite related configurations
        }
      }
    }
    ```
4. Tell the Apprunner plugin which test tasks need the Polaris server
    ```kotlin
    polarisQuarkusApp {
      // Add the name of the test task - usually the same as the name of your test source
      includeTask(tasks.named("polarisServerTest"))
    }
    ```

Note: the above also works within the `:polaris-quarkus-server` project, but the test suite must be neither
`test` nor `intTest` nor `integrationTest`.

### Kotlin DSL / all in one

`build.gradle.kts` - note: the version number needs to be replaced with a (not yet existing) binary release of
Apache Polaris.

```kotlin
plugins {
  `java-library`
  // Replace the version with a binary release of Polaris.
  // ATTENTION! Within the Polaris repository do _NOT_ add the plugin version, it's implicit !
  id("org.apache.polaris.apprunner") version "0.0.0"
}

dependencies {
  // specify the GAV of the Polaris Quarkus server runnable (uber-jar)
  polarisQuarkusServer("org.apache.polaris:polaris-quarkus-server:0.0.0:runner")
}

polarisQuarkusApp {
  // Ensure that the `test` task has a Polaris Server available.  
  includeTask(tasks.named<Test>("test"))
  // Note: prefer setting up separate `polarisServerIntegrationTest` test suite (the name is up to you!),
  // see https://docs.gradle.org/current/userguide/jvm_test_suite_plugin.html

  // Override the default Java version (21) to run the Polaris server / Quarkus.
  // Must be at least 21!
  javaVersion.set(21)
  // Additional environment variables for the Polaris server / Quarkus
  // (Added to the Gradle inputs used to determine Gradle's UP-TO-DATE status)
  environment.put("MY_ENV_VAR", "value")
  // Additional environment variables for the Polaris server / Quarkus
  // (NOT a Gradle inputs used to determine Gradle's UP-TO-DATE status)
  // Put system specific variables here
  environmentNonInput.put("MY_ENV_VAR", "value")
  // System properties for the Polaris server / Quarkus
  // (Added to the Gradle inputs used to determine Gradle's UP-TO-DATE status)
  systemProperties.put("my.sys.prop", "value")
  // System properties for the Polaris server / Quarkus
  // (NOT a Gradle inputs used to determine Gradle's UP-TO-DATE status)
  // Put system specific variables here
  systemPropertiesNonInput.put("my.sys.prop", "value")
  // JVM arguments for the Polaris server JVM (list of strings)
  // (Added to the Gradle inputs used to determine Gradle's UP-TO-DATE status)
  jvmArguments.add("some-arg")
  // JVM arguments for the Polaris server JVM (list of strings)
  // (NOT a Gradle inputs used to determine Gradle's UP-TO-DATE status)
  // Put system specific variables here
  jvmArgumentsNonInput.add("some-arg")
  // Use this (full) path to the executable jar of the Polaris server.
  // Note: This option should generally be avoided in build scripts, prefer the 'polarisQuarkusServer'
  // configuration mentioned above.
  executableJar = file("/my/custom/polars-quarkus-server.jar")
  // Override the working directory for the Polaris Quarkus server, defaults to `polaris-quarkus-server/`
  // in the Gradle project's `build/` directory.
  workingDirectory = file("/i/want/it/to/run/here")
  // override the default timeout of 30 seconds to wait for the Polaris Quarkus Server to emit the
  // listen URLs.
  timeToListenUrlMillis = 30000
  // Override the default timeout of 15 seconds to wait for the Polaris Quarkus Server to stop before
  // it is forcefully killed
  timeToStopMillis = 15000
  // Arguments for the Polaris server (list of strings)
  // (Added to the Gradle inputs used to determine Gradle's UP-TO-DATE status)
  arguments.add("some-arg")
  // Arguments for the Polaris server (list of strings)
  // (NOT a Gradle inputs used to determine Gradle's UP-TO-DATE status)
  // Put system specific variables here
  argumentsNonInput.add("some-arg")
  // The following options can be used to use different property names than described above
  // in this README
  httpListenPortProperty = "quarkus.http.test-port"
  httpListenUrlProperty = "quarkus.http.test-url"
  managementListenPortProperty = "quarkus.management.test-port"
  managementListenUrlProperty = "quarkus.management.test-url"
}
```

### Groovy DSL

`build.gradle` - note: the version number needs to be replaced with a (not yet existing) binary release of
Apache Polaris.

```groovy
plugins {
  id 'java-library'
  id 'org.apache.polaris.apprunner' version "0.0.0"
}

dependencies {
  // specify the GAV of the Polaris Quarkus server runnable (uber-jar)
  polarisQuarkusServer "org.apache.polaris:polaris-quarkus-server:0.0.0:runner"
}

polarisQuarkusApp {
  // Ensure that the `test` task has a Polaris Server available when the tests run.
  includeTask(tasks.named("test"))
  // Note: prefer setting up separate `polarisServerIntegrationTest` test suite (the name is up to you!),
  // see https://docs.gradle.org/current/userguide/jvm_test_suite_plugin.html

  // See the Kotlin DSL description above for information about the options
}
```

## Maven

The `org.apache.polaris.apprunner:polaris-apprunner-maven-plugin` Maven plugin should be used together with the
standard `maven-failsafe-plugin`

`pom.xml` - note: the version number needs to be replaced with a (not yet existing) binary release of
Apache Polaris.

```xml
<project>
  <build>
    <plugins>
      <plugin>
        <!-- The Polaris Apprunner Maven plugin should be used for integration tests -->
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-failsafe-plugin</artifactId>
      </plugin>

      <plugin>
        <groupId>org.apache.polaris.apprunner</groupId>
        <artifactId>polaris-apprunner-maven-plugin</artifactId>
        <!-- TODO replace the version -->>
        <version>0.0.0</version>
        <configuration>
          <!-- Preferred way, specify the GAV of the Polaris Quarkus server (uber-jar) -->
          <!-- TODO replace the version -->>
          <appArtifactId>org.apache.polaris:polaris-quarkus-server:jar:runner:0.0.0</appArtifactId>
          <!-- The system properties passed to the Polaris server -->
          <systemProperties>
            <foo>bar</foo>
          </systemProperties>
          <!-- The environment variables passed to the Polaris server -->
          <environment>
            <HELLO>world</HELLO>
          </environment>
          <!--
            More options:
             <executableJar>  (string)          Use this (full) path to the executable jar of the Polaris server.
                                                Note: This option should generally be avoided in build scripts, 
                                                prefer the 'appArtifactId' option mentioned above.
             <javaVersion>    (int)             Override the default Java version (21)
             <jvmArguments>   (list<string>)    Additional JVM arguments for the Polaris server JVM
             <arguments>      (list<string>)    Additional application arguments for the Polaris server
             <workingDirectory>  (string)       Path of the working directory of the Polaris server,
                                                defaults to "${build.directory}/polaris-quarkus".
             <httpListenPortProperty>           Override the default 'quarkus.http.test-port' property name
             <httpListenUrlProperty>            Override the default 'quarkus.http.test-url' property name
             <managementListenPortProperty>     Override the default 'quarkus.http.management-port' property name
             <managementListenUrlProperty>      Override the default 'quarkus.http.management-url' property name
          --> 
        </configuration>
        <executions>
          <execution>
            <!-- Start the Polaris Server before the integration tests start -->
            <id>start</id>
            <phase>pre-integration-test</phase>
            <goals>
              <goal>start</goal>
            </goals>
          </execution>
          <execution>
            <!-- Stop the Polaris Server after the integration tests finished -->
            <id>stop</id>
            <phase>post-integration-test</phase>
            <goals>
              <goal>stop</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

## Implicit Quarkus options

The plugins always pass the following configuration options as system properties to Quarkus:

```
quarkus.http.port=0
quarkus.management.port=0
quarkus.log.level=INFO
quarkus.log.console.level=INFO
```

Those are meant to let Quarkus bind to a random-ish port, so that the started instances do not conflict with anything
else running on the system and that the necessary log line containing the listen-URLs gets emitted.

You can explicitly override those via the `systemProperties` options of the Gradle and Maven plugins.

## Developing the plugins

The Polaris Apprunner plugins are built via a Gradle "included build" (composite build). As generally with composite
builds, task selection via `gradlew` does _not_ get propagated to included builds. This is especially true for
tasks like `spotlessApply`.

In other words, running `./gradlew spotlessApply` against the "main" Polaris build will run `spotlessApply`
_only_ in the projects in the "main" Polaris build, but _not_ in the apprunner build. This is also true for other
tasks like `check`.

This means, the easiest way is to just change the current working directory to `tools/apprunner` and work from there. 
Publishing the plugins also has to be done from the `tools/apprunner` directory. This is why `gradlew` & co are
present in `tools/apprunner`.

## FAQ

### Does it have to be a Polaris Quarkus server?

The plugins work with any Quarkus application that listens for HTTP requests.

The only requirement of the Polaris Apprunner plugins for the runnable jar is that it is a Quarkus (web) server,
that emits at least the HTTP listen URL (and optionally the management listen URL).

This means, that this plugin can be used with basically any Quarkus based application, whether it's Apache Polaris
or Nessie or something else.

### The plugin always times out starting the server, even if the server starts up

Make sure that Quarkus emits a line like the following:

```
2025-01-16 13:29:25,959 INFO  [io.quarkus] (main) Apache Polaris Server (incubating) 1.0.0-incubating-SNAPSHOT on JVM (powered by Quarkus 3.17.7) started in 0.998s. Listening on: http://0.0.0.0:8181. Management interface listening on http://0.0.0.0:8182.
```

The important part is `Listening on: http://0.0.0.0:8181. Management interface listening on http://0.0.0.0:8182.`,
especially the `Listening on: http://0.0.0.0:8181.` is mandatory, the port number used by Quarkus does not matter.

If that line does not get logged to stdout, the Polaris Apprunner plugin does not detect the Quarkus application to
be running. Make sure that your Quarkus logging configuration allows logging this line to stdout.

The plugins do their best to enforce that, 

## Origin of the Polaris Apprunner plugins

The Polaris Apprunner Gradle and Maven plugins are based
on [projectnessie's apprunner](https://github.com/projectnessie/nessie-apprunner).
